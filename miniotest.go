package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
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
var useTempFileVSStream = true

func main() {
	serverMode := true
	endpoint := "localhost:9000"
	accessKeyID := "9V25FKN0JY7IQZUW85RH"
	secretAccessKey := "wckkTpC3lZ5QYqY0jIJXFJ6XEUsmD1nBCZK7vmva"
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

		r := mux.NewRouter()
		r.HandleFunc("/", rootHandler)
		r.Handle("/upload", &uploadHandler{s3Client: s3client, bucket: &bucket,
			objectName: &objectName, tempdir: &tempdir})
		log.Println(http.ListenAndServe(":20000", r))
	} else {
		doAWS(endpoint, accessKeyID, secretAccessKey, useSSL, bucket, region, objectName, filePath,
			contentType)
		//doMinio(endpoint, accessKeyID, secretAccessKey, useSSL, bucket, region, objectName,
		//	filePath, contentType)
	}
}

func rootHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Hello world")
}

type uploadHandler struct {
	s3Client   *s3.S3
	bucket     *string
	objectName *string
	tempdir    *string
}

func (h *uploadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	data := r.Body
	if useTempFileVSStream {
		tmpPath := fmt.Sprintf("%s/%d%d", *h.tempdir, rand.Int(), rand.Int())
		if tmpFile, err := os.Create(tmpPath); err == nil {
			defer os.Remove(tmpPath) // defers are LIFO
			defer tmpFile.Close()
			io.Copy(tmpFile, data)
			tmpFile.Close()
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

	objmeta, err := h.s3Client.HeadObject(&s3.HeadObjectInput{Bucket: h.bucket, Key: h.objectName})
	if err != nil {
		fmt.Fprintf(w, "failed to get object metadata, %v\n", err)
		return
	}
	fmt.Fprintf(w, "file metadata:\n%v", objmeta)
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

	objmeta, err := svc.HeadObject(&s3.HeadObjectInput{Bucket: &bucket, Key: &objectName})
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

	log.Printf("%#v\n", minioClient) // minioClient is now setup

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

	// Upload the zip file

	// Upload the zip file with FPutObject
	n, err := minioClient.FPutObject(bucket, objectName, filePath,
		minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Successfully uploaded %s of size %d\n", objectName, n)
}
