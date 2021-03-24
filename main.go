package main

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
)

func main() {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)
	if err != nil {
		log.Fatal(err)
	}

	// Create S3 service client
	svc := s3.New(sess)
	svc.Handlers.Build.PushBack(deleteObjectsBodyHandler)

	payload := makeDeleteObjectsPayload()
	url := getDeleteObjectsPresignedUrl(svc, payload.getKeys())

	response, err := deleteObjects(url, payload)
	if err != nil {
		log.Fatal(err.Error())
	}

	if response.StatusCode != 200 {
		body, _ := ioutil.ReadAll(response.Body)
		log.Fatalf("Failed to DELETE objects using a pre-signed URL: status: %d, body: %s",
			response.StatusCode, body)
	}
}

func getDeleteObjectsPresignedUrl(svc *s3.S3, keys []string) string {
	objects := make([]*s3.ObjectIdentifier, len(keys))
	for i, k := range keys {
		objects[i] = &s3.ObjectIdentifier{
			Key: aws.String(k),
		}
	}
	req, _ := svc.DeleteObjectsRequest(&s3.DeleteObjectsInput{
		Bucket: aws.String("test-bucket"),
		Delete: &s3.Delete{
			Objects: objects,
		},
	})

	url, err := req.Presign(15 * time.Second)
	if err != nil {
		log.Fatal(err.Error())
	}
	return url
}

func makeDeleteObjectsPayload() deleteObjectsPayload {
	size := 2
	objects := make([]object, size)
	for i := 0; i < size; i++ {
		objects[i] = object{
			Key: "key-" + strconv.Itoa(i),
		}
	}

	return deleteObjectsPayload{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Object: objects,
	}
}

func deleteObjects(presignedUrl string, payload deleteObjectsPayload) (*http.Response, error) {
	b, _ := xml.Marshal(payload)
	bWithXml := append(append([]byte(xml.Header+string(b)), '\n'))

	req, _ := http.NewRequest("POST", presignedUrl, bytes.NewBuffer(bWithXml))
	req.Header.Add("content-md5", calculateMd5Hash(bWithXml))
	req.Header.Add("content-length", strconv.Itoa(len(bWithXml)))

	client := &http.Client{}
	response, err := client.Do(req)
	return response, err
}

type deleteObjectsPayload struct {
	Xmlns   string   `xml:"xmlns,attr"`
	XMLName xml.Name `xml:"Delete"`
	Object  []object `xml:"Object"`
}

type object struct {
	Key string
}

func (payload deleteObjectsPayload) getKeys() []string {
	size := len(payload.Object)
	keys := make([]string, size)

	for i := 0; i < size; i++ {
		keys[i] = payload.Object[i].Key

	}
	return keys
}

func calculateMd5Hash(payloadBytes []byte) string {
	md5Sum := md5.Sum(payloadBytes)
	md5Hash := base64.StdEncoding.EncodeToString(md5Sum[:])
	return md5Hash
}

func deleteObjectsBodyHandler(r *request.Request) {
	if r.Operation.Name != "DeleteObjects" {
		return
	}

	body, _ := ioutil.ReadAll(r.Body)
	bodyWithXmlHeader := append(append([]byte(xml.Header), body...), '\n')
	r.SetBufferBody(bodyWithXmlHeader)
}
