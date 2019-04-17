package main

import (
	"fmt"
	"log"
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

func main() {
	serverMode := false
	endpoint := "localhost:9000"
	accessKeyID := "9V25FKN0JY7IQZUW85RH"
	secretAccessKey := "wckkTpC3lZ5QYqY0jIJXFJ6XEUsmD1nBCZK7vmva"
	useSSL := false

	bucket := "mybukkit"
	region := "us-west-1"

	objectName := "somefile"
	filePath := "/home/crushingismybusiness/largefile.crap"
	contentType := "text/plain"

	if serverMode {
		r := mux.NewRouter()
		r.HandleFunc("/", helloWorld)
		log.Println(http.ListenAndServe(":20000", r))
	} else {
		doAWS(endpoint, accessKeyID, secretAccessKey, useSSL, bucket, region, objectName, filePath,
			contentType)
		//doMinio(endpoint, accessKeyID, secretAccessKey, useSSL, bucket, region, objectName,
		//	filePath, contentType)
	}
}

func helloWorld(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello world")
}

type rootHandler struct {
	s3Client   *s3.S3
	objectName *string
}

func (h *rootHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello world")
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
		u.PartSize = 4.5 * 1024 * 1024 * 1024 // 50MB per part
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
