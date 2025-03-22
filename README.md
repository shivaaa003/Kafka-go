KAFKA-GO
1. Ensure you have `go` installed locally
2. Run `./your_program.sh` to run your Kafka broker, which is implemented in
   `app/server.go`.
3. Commit your changes and run `git push origin master` to submit your solution
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
