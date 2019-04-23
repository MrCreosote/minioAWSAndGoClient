package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/gorilla/mux"
	"github.com/minio/minio-go"
)

var partSize int64 = 5 * 1024 * 1024 // 5MB per part
var uploadConcurrency = 0
var useTempFileVSStream = false
var clientType = "presign" // should be an enum really
var serverMode = true

func main() {
	// TODO use http2 h2c - make configurable and off by default
	endpoint, accessKeyID, secretAccessKey, err := getConfig(os.Args[1])
	if err != nil {
		log.Fatalln(err)
	}
	// endpoint := "localhost:9000"
	// accessKeyID := "9V25FKN0JY7IQZUW85RH"
	// secretAccessKey := "wckkTpC3lZ5QYqY0jIJXFJ6XEUsmD1nBCZK7vmva"
	useSSL := false

	bucket := "mybukkit"
	region := "us-west-1"

	objectName := "somefile"
	filePath := "/home/crushingismybusiness/largefile.crap"
	contentType := "text/plain"
	tempdir := "temp_dir_for_test"

	if serverMode {
		err := os.MkdirAll(tempdir, 0700)
		if err != nil {
			log.Fatal(err)
		}
		s3client := createS3Client(endpoint, accessKeyID, secretAccessKey, useSSL, region)
		err = createBucketAWS(s3client, bucket)
		if err != nil {
			fmt.Println(err)
			return
		}

		minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
		if err != nil {
			log.Fatalln(err)
		}

		r := mux.NewRouter()
		r.HandleFunc("/", rootHandler)
		r.Handle("/upload/{client}", &uploadHandler{
			s3Client:    s3client,
			minioClient: minioClient,
			bucket:      &bucket,
			objectName:  &objectName,
			contentType: &contentType,
			tempdir:     &tempdir})
		log.Println(http.ListenAndServe(":20000", r))
	} else if clientType == "minio" {
		doMinio(endpoint, accessKeyID, secretAccessKey, useSSL, bucket, region, objectName,
			filePath, contentType)
	} else if clientType == "aws" {
		doAWS(endpoint, accessKeyID, secretAccessKey, useSSL, bucket, region, objectName, filePath,
			contentType)
	} else {
		doPresign(endpoint, accessKeyID, secretAccessKey, useSSL, bucket, region, objectName,
			filePath)
	}
}

func getConfig(host string) (string, string, string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", "", "", err
	}
	cfgpath := home + "/.mc/config.json"
	cfgfile, err := os.Open(cfgpath)
	if err != nil {
		return "", "", "", err
	}
	defer cfgfile.Close()
	byteValue, err := ioutil.ReadAll(cfgfile)
	if err != nil {
		return "", "", "", err
	}

	var result map[string]interface{}
	err = json.Unmarshal([]byte(byteValue), &result)
	if err != nil {
		return "", "", "", err
	}
	hosts := result["hosts"].(map[string]interface{})
	hostmap, found := hosts[host].(map[string]interface{})
	if !found {
		return "", "", "", fmt.Errorf("No host %s found in mc config file %s", host, cfgpath)
	}
	hosturl := hostmap["url"].(string)
	hostsplt := strings.Split(hosturl, "/")
	hosturl = hostsplt[len(hostsplt)-1]
	accessKey := hostmap["accessKey"].(string)
	secretKey := hostmap["secretKey"].(string)

	return hosturl, accessKey, secretKey, nil
}

func rootHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Hello world")
}

type uploadHandler struct {
	s3Client    *s3.S3
	minioClient *minio.Client
	bucket      *string
	objectName  *string
	contentType *string
	tempdir     *string
}

func (h *uploadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	client := mux.Vars(r)["client"] // must be found
	if client == "dumprequest" {
		fmt.Fprintf(w, "Dumping request and returning\n")
		r.Write(w)
		return
	}
	if client == "parseform" {
		fmt.Fprintf(w, "Parsing multipart form and returning\n")
		parseMultipart(w, r)
		return
	}
	if client == "presign" {
		fmt.Fprint(w, "Uploading with presigned URL\n")
		loadWithPresign(h, w, r)
		return
	}
	if client == "get" {
		getObject(h, w, r)
		return
	}
	var useMinio bool
	if client == "aws" {
		useMinio = false
	} else if client == "minio" {
		useMinio = true
	} else {
		fmt.Fprintf(w, "Illegal client value (ok: [aws, minio]): %s\n", client)
		return
	}
	var pSize int64
	pSizeStr, found := r.URL.Query()["partsize"]
	if !found {
		pSize = partSize
	} else {
		pSize2, err := strconv.ParseInt(pSizeStr[0], 10, 64)
		if err != nil {
			fmt.Fprintf(w, "Illegal part size: %s\n", pSizeStr[0])
			return
		}
		pSize = pSize2
	}
	fmt.Fprintf(w, "Using part size %d\n", pSize)
	if useMinio {
		fmt.Fprint(w, "Using Minio client\n")
		loadWithMinio(h, w, r, pSize)
	} else {
		fmt.Fprint(w, "Using AWS client\n")
		loadWithAWS(h, w, r, pSize)
	}
}

func getObject(h *uploadHandler, w http.ResponseWriter, r *http.Request) {

	result, err := h.s3Client.GetObject(&s3.GetObjectInput{
		Bucket: h.bucket,
		Key:    h.objectName,
	})
	if err != nil {
		fmt.Fprintf(w, "Error getting file: %v\n", err)
		return
	}
	defer result.Body.Close()

	io.Copy(w, result.Body)
}

func parseMultipart(w http.ResponseWriter, r *http.Request) {
	multiReader, err := r.MultipartReader()
	if err != nil {
		fmt.Fprint(w, "Not a multipart form\n")
		return
	}
	defer r.Body.Close()
	for {
		part, err := multiReader.NextPart()
		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Fprintf(w, "Error parsing multipart form: %v\n", err)
			return
		}
		defer part.Close()
		fmt.Fprintf(w, "*** Part formname: %s, filename: %s***\n",
			part.FormName(), part.FileName())
		fmt.Fprintf(w, "Part headers:\n%v\n", part.Header)
		contentLength := part.Header.Get("Content-Length")
		if contentLength != "" {
			fmt.Fprintf(w, "Content-Length was %s\n", contentLength)
		} else {
			fmt.Fprintf(w, "No Content-Length header\n")
		}
		fmt.Fprintf(w, "Part body:\n")
		io.Copy(w, part)
	}
}

func loadWithPresign(h *uploadHandler, w http.ResponseWriter, r *http.Request) {
	var contentLength int64
	cl := r.Header.Get("content-length")
	if cl == "" {
		contentLength = -1
	} else {
		cl2, err := strconv.ParseInt(cl, 10, 64)
		if err != nil {
			fmt.Fprintf(w, "Illegal content-length: %s\n", cl)
			return
		}
		contentLength = cl2
	}

	data := r.Body
	if useTempFileVSStream {
		tmpPath := fmt.Sprintf("%s/%d%d", *h.tempdir, rand.Int(), rand.Int())
		fmt.Fprintf(w, "using temp file %s\n", tmpPath)
		if tmpFile, err := os.Create(tmpPath); err == nil {
			defer os.Remove(tmpPath) // defers are LIFO
			defer tmpFile.Close()
			defer data.Close()
			io.Copy(tmpFile, data)
			tmpFile.Close()
			data.Close()
			if tmpFile2, err2 := os.Open(tmpPath); err2 == nil {
				data = tmpFile2
				defer tmpFile2.Close()
			} else {
				fmt.Fprintf(w, "Error opening temp file: %v", err)
				return
			}
		} else {
			fmt.Fprintf(w, "Error opening temp file: %v", err)
			return
		}
	}

	putObj, _ := h.s3Client.PutObjectRequest(&s3.PutObjectInput{ // PutObjectOutput is never filled
		Bucket: h.bucket,
		Key:    h.objectName,
	})

	url, _, err := putObj.PresignRequest(15 * time.Minute) // headers is nil in this case
	if err != nil {
		fmt.Fprintf(w, "error presigning request: %s\n", err)
		return
	}
	// need to put a timeout on this. If the content-length is wrong, it'll hang forever
	// actually no. It hangs forever in curl for 50MB CL on a 12GB file, doesn't hang in this code
	req, err := http.NewRequest("PUT", url, data)
	if err != nil {
		fmt.Fprintf(w, "error creating request: %s\n", err)
		return
	}
	req.ContentLength = contentLength

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(w, "error executing request: %s\n", err)
	} else {
		fmt.Fprintf(w, "response for request\n")
		resp.Write(w)
		writeObjectMeta(h, w)
	}

}

func loadWithAWS(h *uploadHandler, w http.ResponseWriter, r *http.Request, partSize int64) {
	data := r.Body
	if useTempFileVSStream {
		tmpPath := fmt.Sprintf("%s/%d%d", *h.tempdir, rand.Int(), rand.Int())
		fmt.Fprintf(w, "using temp file %s\n", tmpPath)
		if tmpFile, err := os.Create(tmpPath); err == nil {
			defer os.Remove(tmpPath) // defers are LIFO
			defer tmpFile.Close()
			defer data.Close()
			io.Copy(tmpFile, data)
			tmpFile.Close()
			data.Close()
			if tmpFile2, err2 := os.Open(tmpPath); err2 == nil {
				data = tmpFile2
				defer tmpFile2.Close()
			} else {
				fmt.Fprintf(w, "Error opening temp file: %v", err)
				return
			}
		} else {
			fmt.Fprintf(w, "Error opening temp file: %v", err)
			return
		}
	}
	uploader := s3manager.NewUploaderWithClient(h.s3Client, func(u *s3manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = uploadConcurrency
	})

	// Upload the file to S3.
	uploadStart := time.Now()
	objresult, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: h.bucket,
		Key:    h.objectName,
		Body:   data,
	})
	fmt.Fprintf(w, "upload took %s\n", time.Since(uploadStart))
	if err != nil {
		fmt.Fprintf(w, "failed to upload file, %v\n", err)
		return
	}
	fmt.Fprintf(w, "file uploaded to %s\n", objresult.Location)

	writeObjectMeta(h, w)
}

func writeObjectMeta(h *uploadHandler, w http.ResponseWriter) {
	objmeta, err := h.s3Client.HeadObject(&s3.HeadObjectInput{Bucket: h.bucket, Key: h.objectName})
	if err != nil {
		fmt.Fprintf(w, "failed to get object metadata, %v\n", err)
		return
	}
	fmt.Fprintf(w, "file metadata:\n%v\n", objmeta)
}

func loadWithMinio(h *uploadHandler, w http.ResponseWriter, r *http.Request, partSize int64) {
	var uploadStart time.Time
	var n int64
	if useTempFileVSStream {
		tmpPath := fmt.Sprintf("%s/%d%d", *h.tempdir, rand.Int(), rand.Int())
		fmt.Fprintf(w, "using temp file %s\n", tmpPath)
		if tmpFile, err := os.Create(tmpPath); err == nil {
			defer os.Remove(tmpPath) // defers are LIFO
			defer tmpFile.Close()
			defer r.Body.Close()
			io.Copy(tmpFile, r.Body)
			tmpFile.Close()
			defer r.Body.Close()
		} else {
			fmt.Fprintf(w, "Error opening temp file: %v", err)
			return
		}
		uploadStart = time.Now()
		n2, err := h.minioClient.FPutObject(*h.bucket, *h.objectName, tmpPath,
			minio.PutObjectOptions{
				ContentType: *h.contentType,
				PartSize:    uint64(partSize)})
		if err != nil {
			fmt.Fprintf(w, "faled to upload data %v\n", err)
			return
		}
		n = n2
	} else {
		uploadStart = time.Now()
		n2, err := h.minioClient.PutObject(*h.bucket, *h.objectName, r.Body, -1,
			minio.PutObjectOptions{
				ContentType: *h.contentType,
				PartSize:    uint64(partSize)})
		if err != nil {
			fmt.Fprintf(w, "faled to upload data %v\n", err)
			return
		}
		n = n2
	}
	fmt.Fprintf(w, "upload took %s\n", time.Since(uploadStart))
	fmt.Fprintf(w, "Successfully uploaded %s of size %d\n", *h.objectName, n)
	meta, err := h.minioClient.StatObject(*h.bucket, *h.objectName, minio.StatObjectOptions{})
	if err != nil {
		fmt.Fprintf(w, "failed to stat data %v\n", err)
		return
	}
	fmt.Fprintf(w, "file metadata:\n%v\n", meta)
}

func createBucketAWS(s3Client *s3.S3, bucket string) error {
	input := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	_, err := s3Client.CreateBucket(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				log.Println("Bucket already exists")
				return nil // everything's groovy
			default:
				// do nothing
			}
		}
		return err
	}
	return nil
}

func createS3Client(
	endpoint string,
	accessKeyID string,
	secretAccessKey string,
	useSSL bool,
	region string) *s3.S3 {

	trueref := true
	disableSSL := !useSSL

	sess := session.Must(session.NewSession())
	creds := credentials.NewStaticCredentials(accessKeyID, secretAccessKey, "")
	svc := s3.New(sess, &aws.Config{
		Credentials:      creds,
		Endpoint:         &endpoint,
		Region:           &region,
		DisableSSL:       &disableSSL, // detect correct setting based on url prefix, warn for http
		S3ForcePathStyle: &trueref})   // minio pukes otherwise
	return svc
}

func doAWS(
	endpoint string,
	accessKeyID string,
	secretAccessKey string,
	useSSL bool,
	bucket string,
	region string,
	objectName string,
	filePath string,
	contentType string) {

	svc := createS3Client(endpoint, accessKeyID, secretAccessKey, useSSL, region)

	err := createBucketAWS(svc, bucket)
	if err != nil {
		fmt.Println(err)
		return
	}

	uploader := s3manager.NewUploaderWithClient(svc, func(u *s3manager.Uploader) {
		u.PartSize = partSize
	})

	f, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("failed to open file %q, %v\n", filePath, err)
		return
	}

	// Upload the file to S3.
	uploadStart := time.Now()
	objresult, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectName),
		Body:   f,
	})
	log.Printf("upload took %s\n", time.Since(uploadStart))
	if err != nil {
		fmt.Printf("failed to upload file, %v\n", err)
		return
	}
	fmt.Printf("file uploaded to %s\n", objresult.Location)

	printObjectMeta(svc, bucket, objectName)
}

func printObjectMeta(client *s3.S3, bucket string, objectName string) {
	objmeta, err := client.HeadObject(&s3.HeadObjectInput{Bucket: &bucket, Key: &objectName})
	if err != nil {
		fmt.Printf("failed to get object metadata, %v\n", err)
		return
	}
	fmt.Printf("file metadata:\n%v", objmeta)
}

func doMinio(
	endpoint string,
	accessKeyID string,
	secretAccessKey string,
	useSSL bool,
	bucket string,
	location string,
	objectName string,
	filePath string,
	contentType string) {
	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		log.Fatalln(err)
	}

	err = minioClient.MakeBucket(bucket, location)
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, err := minioClient.BucketExists(bucket)
		if err == nil && exists {
			log.Printf("We already own %s\n", bucket)
		} else {
			log.Fatalln(err)
		}
	} else {
		log.Printf("Successfully created %s\n", bucket)
	}

	uploadStart := time.Now()
	n, err := minioClient.FPutObject(bucket, objectName, filePath,
		minio.PutObjectOptions{
			ContentType: contentType,
			PartSize:    uint64(partSize)})
	log.Printf("upload took %s\n", time.Since(uploadStart))
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Successfully uploaded %s of size %d\n", objectName, n)
	meta, err := minioClient.StatObject(bucket, objectName, minio.StatObjectOptions{})
	if err != nil {
		log.Printf("faied to stat data %v\n", err)
		return
	}
	log.Printf("file metadata:\n%v\n", meta)
}

func doPresign(
	endpoint string,
	accessKeyID string,
	secretAccessKey string,
	useSSL bool,
	bucket string,
	region string,
	objectName string,
	filePath string) {

	svc := createS3Client(endpoint, accessKeyID, secretAccessKey, useSSL, region)

	err := createBucketAWS(svc, bucket)
	if err != nil {
		fmt.Println(err)
		return
	}

	f, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("failed to open file %q, %v\n", filePath, err)
		return
	}
	fstat, _ := f.Stat()
	contentLength := fstat.Size()

	putObj, _ := svc.PutObjectRequest(&s3.PutObjectInput{ // PutObjectOutput is never filled
		Bucket: &bucket,
		Key:    &objectName,
	})

	url, _, err := putObj.PresignRequest(15 * time.Minute) // headers is nil in this case
	if err != nil {
		fmt.Printf("error presigning request: %s\n", err)
		return
	}
	req, err := http.NewRequest("PUT", url, f)
	if err != nil {
		fmt.Printf("error creating request: %s\n", err)
		return
	}
	req.ContentLength = contentLength

	uploadStart := time.Now()
	resp, err := http.DefaultClient.Do(req)
	log.Printf("upload took %s\n", time.Since(uploadStart))
	if err != nil {
		fmt.Printf("error executing request: %s\n", err)
	} else {
		fmt.Printf("response for request\n")
		resp.Write(os.Stdout)
		printObjectMeta(svc, bucket, objectName)
	}
}
