package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/minio/minio-go"
)

func main() {
	endpoint := "localhost:9000"
	accessKeyID := "9V25FKN0JY7IQZUW85RH"
	secretAccessKey := "wckkTpC3lZ5QYqY0jIJXFJ6XEUsmD1nBCZK7vmva"
	useSSL := false

	bucket := "mybukkit"
	region := "us-west-1"

	objectName := "somefile"
	filePath := "/home/crushingismybusiness/examples.desktop"
	contentType := "text/plain"

	doAWS(endpoint, accessKeyID, secretAccessKey, useSSL, bucket, region, objectName, filePath,
		contentType)
	//doMinio(endpoint, accessKeyID, secretAccessKey, useSSL, bucket, region, objectName,
	//	filePath, contentType)
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

	trueref := true

	sess := session.Must(session.NewSession())
	creds := credentials.NewStaticCredentials(accessKeyID, secretAccessKey, "")
	svc := s3.New(sess, &aws.Config{
		Credentials:      creds,
		Endpoint:         &endpoint,
		Region:           &region,
		DisableSSL:       &trueref,  // detect correct setting based on url prefix, warn for http
		S3ForcePathStyle: &trueref}) // minio pukes otherwise

	input := &s3.CreateBucketInput{Bucket: aws.String(bucket)}

	result, err := svc.CreateBucket(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
				fmt.Println(s3.ErrCodeBucketAlreadyExists, aerr.Error())
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				// should keep going here
				fmt.Println(s3.ErrCodeBucketAlreadyOwnedByYou, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	fmt.Println(result)

	uploader := s3manager.NewUploaderWithClient(svc)

	f, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("failed to open file %q, %v\n", filePath, err)
		return
	}

	// Upload the file to S3.
	objresult, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectName),
		Body:   f,
	})
	if err != nil {
		fmt.Printf("failed to upload file, %v\n", err)
		return
	}
	fmt.Printf("file uploaded to, %s\n", objresult.Location)

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
