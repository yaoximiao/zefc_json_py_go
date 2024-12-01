package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"github.com/klauspost/reedsolomon"
	"github.com/gocarina/gocsv"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

type EncodingMetadata struct {
	K               int      `json:"k"`
	M               int      `json:"m"`
	ShardSize       int      `json:"shard_size"`
	OriginalLength int      `json:"original_length"`
	Shares          []string `json:"shares"`
}

type DataFrame struct {
	Age         []int     `csv:"age"`
	Income      []float64 `csv:"income"`
	CreditScore []int     `csv:"credit_score"`
}

type Person struct {
	Age         int     `json:"age"`
	Income      float64 `json:"income"`
	CreditScore int     `json:"credit_score"`
}

func decodeCtganData(filename string) ([]byte, error) {
	// 1. 读取 JSON 文件
	fmt.Println("1. 读取 JSON 文件")
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	// 2. 解析元数据
	fmt.Println("2. 解析元数据")
	var metadata EncodingMetadata
	err = json.Unmarshal(data, &metadata)
	if err != nil {
		return nil, err
	}

	fmt.Println("解码参数：")
	fmt.Printf("数据分片数 (k): %d\n", metadata.K)
	fmt.Printf("总分片数 (m): %d\n", metadata.M)
	fmt.Printf("每个分片大小: %d 字节\n", metadata.ShardSize)
	fmt.Printf("原始数据长度: %d 字节\n", metadata.OriginalLength)

	// 3. 创建 Reed-Solomon 解码器
	fmt.Println("3. 创建 Reed-Solomon 解码器")
	enc, err := reedsolomon.New(metadata.K, metadata.M-metadata.K)
	if err != nil {
		return nil, err
	}

	// 4. 解码 Base64 分片
	fmt.Println("4. 解码 Base64 分片")
	shards := make([][]byte, metadata.K+metadata.M-metadata.K)
	for i, shareBase64 := range metadata.Shares {
		shareBytes, err := base64.StdEncoding.DecodeString(shareBase64)
		if err != nil {
			return nil, err
		}
		shards[i] = shareBytes
	}

	// 5. 重建数据
	fmt.Println("5. 重建数据")
	err = enc.Reconstruct(shards)
	if err != nil {
		return nil, err
	}

	// 6. 合并数据分片
	fmt.Println("6. 合并数据分片")
	recoveredData := make([]byte, 0, metadata.OriginalLength)
	for i := 0; i < metadata.K; i++ {
		shard := shards[i][:metadata.ShardSize]
		recoveredData = append(recoveredData, shard...)
	}

	// 7. 去除填充
	fmt.Println("7. 去除填充")
	recoveredData = recoveredData[:metadata.OriginalLength]

	return recoveredData, nil
}

func saveToCSV(jsonBytes []byte, outputFile string) error {
	// 将 JSON 字节转换为 DataFrame 结构体
	var df DataFrame
	err := json.Unmarshal(jsonBytes, &df)
	if err != nil {
		return err
	}

	// 创建 CSV 文件
	file, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// 使用 gocsv 库将 DataFrame 数据写入 CSV
	err = gocsv.MarshalFile(&df, file)
	if err != nil {
		return err
	}

	fmt.Println("CSV 文件已保存：", outputFile)
	return nil
}

func main() {
	// 解码数据
	recoveredBytes, err := decodeCtganData("ctgan_erasure_code.json")
	if err != nil {
		log.Fatal(err)
	}

	// 将恢复的字节转换回 JSON
	fmt.Println("恢复的 JSON 长度：", len(recoveredBytes))
	// 将恢复的字节转换回 JSON 字符串并打印
	fmt.Println("恢复的 JSON 数据：")
	fmt.Println(string(recoveredBytes))  // 打印恢复的 JSON 内容

	err = ioutil.WriteFile("recovered_ctgan_data.json", recoveredBytes, 0644)
	if err != nil {
		log.Fatal(err)
	}

	var people []Person
	err = json.Unmarshal(recoveredBytes, &people)
	if err != nil {
		log.Fatal(err)
	}

	df := dataframe.LoadStructs(people)

	err = saveToCSV(&df, "recovered_ctgan_data.csv")
	if err != nil {
		log.Fatal(err)
	}

}
