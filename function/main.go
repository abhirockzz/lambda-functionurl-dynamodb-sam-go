package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/jsii-runtime-go"
)

const dynamoDBTableNameEnvVar = "DYNAMODB_TABLE_NAME"
const conditionExpression = "attribute_not_exists(email)"

var tableName string
var client *dynamodb.DynamoDB

func init() {
	tableName = os.Getenv(dynamoDBTableNameEnvVar)
	if tableName == "" {
		log.Fatalf("missing environment variable %s\n", dynamoDBTableNameEnvVar)
	}

	client = dynamodb.New(session.New())
}

type User struct {
	EmailID string `json:"email"`
	Name    string `json:"username,omitempty" dynamodbav:"user_name,omitempty"`
}

func main() {
	lambda.Start(route)
}

func route(ctx context.Context, req events.LambdaFunctionURLRequest) (events.LambdaFunctionURLResponse, error) {
	method := req.RequestContext.HTTP.Method
	//log.Println("method ==", method)

	if method == http.MethodGet {
		return get(ctx, req)
	} else if method == http.MethodPost {
		return create(ctx, req)
	}

	return events.LambdaFunctionURLResponse{StatusCode: http.StatusMethodNotAllowed}, nil
}

func create(ctx context.Context, req events.LambdaFunctionURLRequest) (events.LambdaFunctionURLResponse, error) {
	client := dynamodb.New(session.New())

	var u User

	err := json.Unmarshal([]byte(req.Body), &u)
	if err != nil {
		log.Println("failed to unmarshal request payload", err)
		return events.LambdaFunctionURLResponse{StatusCode: http.StatusBadRequest}, nil
	}

	av, err := dynamodbattribute.MarshalMap(u)

	if err != nil {
		log.Println("failed to marshal struct into dynamodb record", err)
		return events.LambdaFunctionURLResponse{}, err
	}

	_, err = client.PutItem(&dynamodb.PutItemInput{TableName: aws.String(tableName), Item: av, ConditionExpression: jsii.String(conditionExpression)})

	if err != nil {
		log.Println("dynamodb put item failed", err)

		// if the user with same email already exists
		if strings.Contains(err.Error(), dynamodb.ErrCodeConditionalCheckFailedException) {
			log.Printf("user %s already exists\n", u.EmailID)
			return events.LambdaFunctionURLResponse{StatusCode: http.StatusConflict}, nil
		}
		return events.LambdaFunctionURLResponse{}, err
	}

	log.Printf("successfully created user %s\n", u.EmailID)
	return events.LambdaFunctionURLResponse{StatusCode: http.StatusCreated}, nil
}

func get(ctx context.Context, req events.LambdaFunctionURLRequest) (events.LambdaFunctionURLResponse, error) {

	email := req.QueryStringParameters["email"]

	if email == "" {
		return listAllUsers()
	} else {
		return findUser(email)
	}

}

func findUser(email string) (events.LambdaFunctionURLResponse, error) {
	log.Println("searching for user", email)

	output, err := client.GetItem(&dynamodb.GetItemInput{TableName: aws.String(tableName), Key: map[string]*dynamodb.AttributeValue{"email": &dynamodb.AttributeValue{S: aws.String(email)}}})

	if err != nil {
		log.Println("error calling dynamodb get item", err)
		return events.LambdaFunctionURLResponse{}, err
	}

	item := output.Item

	if item == nil {
		log.Printf("user not found %s\n", email)
		return events.LambdaFunctionURLResponse{StatusCode: http.StatusNotFound}, nil
	}

	log.Println("found user", email)

	var u User
	err = dynamodbattribute.UnmarshalMap(item, &u)

	if err != nil {
		log.Println("error calling dynamodbattribute unmarshal map", err)
		return events.LambdaFunctionURLResponse{}, err
	}

	res, err := json.Marshal(u)
	if err != nil {
		log.Println("error calling json marshal for user", err)
		return events.LambdaFunctionURLResponse{}, err
	}

	return events.LambdaFunctionURLResponse{Body: string(res), StatusCode: http.StatusOK}, nil
}

func listAllUsers() (events.LambdaFunctionURLResponse, error) {

	log.Println("listing all items in table...")

	output, err := client.Scan(&dynamodb.ScanInput{TableName: aws.String(tableName)})
	if err != nil {
		log.Println("error calling dynamodb scan", err)
		return events.LambdaFunctionURLResponse{}, err
	}

	items := output.Items

	var users []User
	err = dynamodbattribute.UnmarshalListOfMaps(items, &users)

	if err != nil {
		log.Println("error calling dynamodbattribute unmarshal list of maps", err)
		return events.LambdaFunctionURLResponse{}, err
	}

	res, err := json.Marshal(users)
	if err != nil {
		log.Println("error calling json marshal for users", err)
		return events.LambdaFunctionURLResponse{}, err
	}

	return events.LambdaFunctionURLResponse{Body: string(res), StatusCode: http.StatusOK}, nil
}
