# EOL

This project is not being used (or maintained) and will not be moved when bintray ceases hosting.

# Lagom Kinesis Provider

An implementation of the Lagom Message Broker API for AWS's Kinesis.

This project is currently still an early experiment and is not suitable for production use.

This project is derived from the kafka implementation of the message broker 
API embedded in the lagom project by Lightbend.

# Testing Consumer Locally with Kinesalite

[Kinesalite](https://github.com/mhart/kinesalite) can be used to test a lagom-kinesis consumer locally.
TLS is required for producers, so testing is more involved and not documented here.

1. Install [kinesalite](https://github.com/mhart/kinesalite), [dynalite](https://github.com/mhart/dynalite), and [aws-cli](https://aws.amazon.com/cli/)
2. Run `kinesalite --port 1234` and `dynalite --port 5678` in separate tabs
3. Update your service's configuration file to point to the local kinesalite and dynamodb endpoints
```
lagom.broker.kinesis {
    kinesis-endpoint="http://127.0.0.1:1234"
    dynamodb-endpoint="http://127.0.0.1:5678"
    ...
    client {
    	consumer {
            aws-access-key="put_anything_here"
            aws-secret-key="put_anything_here"
            ...
        }
	} 
}
```
4. Run your service with the following environment variable set: `AWS_CBOR_DISABLE=true`.
The AWS Java client defaults to a binary JSON format (CBOR), which is not compatible with kinesalite.
This setting reverts the default format to plain JSON.
5. That's it! You can use `aws-cli` to create a stream and insert JSON events into it, which should be picked up by your lagom application (assuming your service descriptor's topic name matches the name of the stream you created)

If you prefer to use the AWS Java client to create streams and insert events, the following client configuration can be used:
```
AmazonKinesisClientBuilder.standard()
    .setCredentials(new EnvironmentVariableCredentialsProvider())
	.setClientConfiguration(new ClientConfiguration())
	.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s"http://127.0.0.1:1234", "us-east-1))
    .build()
```
 Make sure you set the following environment variables before running the client:
- `AWS_CBOR_DISABLE=true`
- `AWS_ACCESS_KEY_ID=put_anything_here`
- `AWS_SECRET_KEY=put_anything_here`
