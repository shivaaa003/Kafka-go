[![progress-banner](https://backend.codecrafters.io/progress/kafka/9d65586c-bf32-417b-87ba-5b28ff2595a1)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This is a starting point for Go solutions to the
["Build Your Own Kafka" Challenge](https://codecrafters.io/challenges/kafka).

In this challenge, you'll build a toy Kafka clone that's capable of accepting
and responding to APIVersions & Fetch API requests. You'll also learn about
encoding and decoding messages using the Kafka wire protocol. You'll also learn
about handling the network protocol, event loops, TCP sockets and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

# Passing the first stage

The entry point for your Kafka implementation is in `app/server.go`. Study and
uncomment the relevant code, and push your changes to pass the first stage:

```sh
git commit -am "pass 1st stage" # any msg
git push origin master
```

That's all!

# Stage 2 & beyond

Note: This section is for stages 2 and beyond.

1. Ensure you have `go (1.19)` installed locally
1. Run `./your_program.sh` to run your Kafka broker, which is implemented in
   `app/server.go`.
1. Commit your changes and run `git push origin master` to submit your solution
   to CodeCrafters. Test output will be streamed to your terminal.


# References

1. ApiKeys - https://kafka.apache.org/protocol.html#protocol_api_keys
1. Request and Response - https://kafka.apache.org/protocol.html#protocol_messages
1. ApiVersions - https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
1. DescribePartitions - https://kafka.apache.org/protocol.html#The_Messages_DescribeTopicPartitions
1. Fetch - https://kafka.apache.org/protocol.html#The_Messages_Fetch
1. ErrorCode - https://kafka.apache.org/protocol.html#protocol_error_codes


# Rough Work

```shell
echo -n "000000230012674a4f74d28b00096b61666b612d636c69000a6b61666b612d636c6904302e3100" | xxd -r -p | nc localhost 9092 | hexdump -C


echo "000000230012000450b2a73000096b61666b612d636c69000a6b61666b612d636c6904302e3100" | xxd -r -p | nc localhost 9092 | hexdump -C


# describe partition first
echo -n "00000023001200046f7fc66100096b61666b612d636c69000a6b61666b612d636c6904302e3100" | xxd -r -p | nc localhost 9092 | hexdump -C

# describe topic partition request

echo -n "00000031004b00002aa78ccf000c6b61666b612d746573746572000212756e6b6e6f776e2d746f7069632d7175780000000001ff00" | xxd -r -p | nc localhost 9092 | hexdump -C
```