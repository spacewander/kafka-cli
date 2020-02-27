// Copyright © 2017 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
)

var (
	requiredAcks int16
)

// produceCmd represents the produce command
var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "produce message",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) <= 1 {
			fmt.Println("topic and value are required")
			os.Exit(-1)
		}

		topic := args[0]
		filename := args[1]

		var key string
		if len(args) > 2 {
			key = args[2]
		}

		kafkaClient.Config().Producer.RequiredAcks =
			sarama.RequiredAcks(requiredAcks)
		kafkaClient.Config().Producer.Return.Successes = true
		kafkaClient.Config().Producer.Return.Errors = true
		producer, err := sarama.NewSyncProducerFromClient(kafkaClient)
		exitOnError(err)
		defer producer.Close()

		f, err := os.Open(filename)
		exitOnError(err)
		defer f.Close()
		value, err := ioutil.ReadAll(f)
		exitOnError(err)

		msg := sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(value),
		}
		if key != "" {
			msg.Key = sarama.StringEncoder(key)
		}
		partition, offset, err := producer.SendMessage(&msg)
		exitOnError(err)
		fmt.Printf("Sent to %s, partition: %d, offset: %d\n",
			topic, partition, offset)
	},
}

func init() {
	RootCmd.AddCommand(produceCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// produceCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// produceCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	produceCmd.Flags().Int16Var(&requiredAcks, "request.required.acks", 1,
		"The level of acknowledgement reliability needed from the broker")
}
