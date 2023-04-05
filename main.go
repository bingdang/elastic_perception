package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	cs20151215 "github.com/alibabacloud-go/cs-20151215/v3/client"
	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	util "github.com/alibabacloud-go/tea-utils/v2/service"
	"github.com/alibabacloud-go/tea/tea"
	"io"
	"os"
	"strconv"
	"sync"
)

var (
	wr         sync.WaitGroup
	nodedatach = make(chan cs20151215.DescribeClusterNodesResponseBodyNodes, 200)
)

func HandleError(err error, where string) {
	if err != nil {
		fmt.Println(where, err)
		return
	}
}

func MapToFile(filename string, nodedatamap map[string]cs20151215.DescribeClusterNodesResponseBodyNodes) {
	marshal, err := json.Marshal(nodedatamap)
	HandleError(err, "MapToFile")
	err = Persistent(filename, marshal)
	HandleError(err, "Persistent")
	return
}

func DataDiff(map1, map2 map[string]cs20151215.DescribeClusterNodesResponseBodyNodes, operate string) (err error) {
	if operate == "NewToOld" {
		for k, _ := range map1 {
			if _, ok := map2[k]; ok {
			} else {
				fmt.Println(k, "已创建")
			}
		}
	} else {
		for k, _ := range map1 {
			if _, ok := map2[k]; ok {
			} else {
				fmt.Println(k, "已清退")
			}
		}
	}
	return
}

func FileToMap(filename string) (Oldnodedatamap map[string]cs20151215.DescribeClusterNodesResponseBodyNodes) {
	Olddata, err := ReadAhead(filename)
	HandleError(err, "ReadOldFile")
	err = json.Unmarshal(Olddata, &Oldnodedatamap)
	HandleError(err, "FileToMap")
	return
}

func ReadAhead(filename string) (Olddata []byte, err error) {
	fmt.Println(filename)
	Oldfile, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	HandleError(err, "Openfile")
	defer Oldfile.Close()
	Olddata, err = io.ReadAll(Oldfile)
	return
}

func Persistent(filename string, content []byte) (err error) {
	err = os.WriteFile(filename, content, 0644)
	return
}

func CreateClient(accessKeyId *string, accessKeySecret *string) (_result *cs20151215.Client, _err error) {
	config := &openapi.Config{
		// 必填，您的 AccessKey ID
		AccessKeyId: accessKeyId,
		// 必填，您的 AccessKey Secret
		AccessKeySecret: accessKeySecret,
	}
	// 访问的域名
	config.Endpoint = tea.String("cs.cn-shanghai.aliyuncs.com")
	_result = &cs20151215.Client{}
	_result, _err = cs20151215.NewClient(config)
	return _result, _err
}

// 构建请求
func CreateRequest(args []*string, PageNumber *string, client *cs20151215.Client) (resp *cs20151215.DescribeClusterNodesResponse, _err error) {
	describeClusterNodesRequest := &cs20151215.DescribeClusterNodesRequest{
		InstanceIds: args[3],
		PageNumber:  PageNumber,
		PageSize:    tea.String("20"),
	}
	runtime := &util.RuntimeOptions{}
	headers := make(map[string]*string)
	resp, _err = client.DescribeClusterNodesWithOptions(args[2], describeClusterNodesRequest, headers, runtime)
	if _err != nil {
		return
	}
	return
}

// 获取总分页数
func GetTotalNodes(args []*string, client *cs20151215.Client) (PageCount int32, _err error) {
	//获取总节点数及总分页数
	Home := "1"
	request, _err := CreateRequest(args, &Home, client)

	//总节点数取模如果不为0，则页面数+1
	if *request.Body.Page.TotalCount%*request.Body.Page.PageSize == 0 {
		PageCount = *request.Body.Page.TotalCount / *request.Body.Page.PageSize
	} else {
		PageCount = *request.Body.Page.TotalCount / *request.Body.Page.PageSize + 1
	}

	return
}

// 获取新版数据
func GetNewData(args []*string, client *cs20151215.Client) (nodedatamap map[string]cs20151215.DescribeClusterNodesResponseBodyNodes) {
	nodedatamap = make(map[string]cs20151215.DescribeClusterNodesResponseBodyNodes, 200)
	fmt.Println(nodedatach, "创建新map")
	//获取分页数
	PageNums, _err := GetTotalNodes(args, client)
	HandleError(_err, "GetTotalNodes")
	var PageNum int32
	for PageNum = 1; PageNum <= PageNums; PageNum++ {
		//获取线上所以节点
		PageNumStr := strconv.Itoa(int(PageNum))
		wr.Add(1)
		go GetAllNode(args, PageNumStr, client)
	}

	wr.Wait()
	close(nodedatach)

	//构建新数据map
	_err = GetDataToMap(nodedatamap)
	HandleError(_err, "GetMap")

	return
}

// 协程方式将节点数据存入管道
func GetAllNode(args []*string, PageNumStr string, client *cs20151215.Client) (_err error) {
	resp, _err := CreateRequest(args, &PageNumStr, client)
	HandleError(_err, "GetNodes")
	for _, v := range resp.Body.Nodes {
		nodedatach <- *v
	}
	wr.Done()
	return
}

// 读取管道获取节点数据并构建map
func GetDataToMap(nodedatamap map[string]cs20151215.DescribeClusterNodesResponseBodyNodes) (_err error) {
	for v := range nodedatach {
		nodedatamap[*v.InstanceId] = v
	}
	return
}

func _main(args []*string) (_err error) {

	//判断参数
	if len(args) == 0 {
		fmt.Println("参数为空")
		return
	}

	//解密base64
	for i := 0; i < 2; i++ {
		vByte, _err := base64.StdEncoding.DecodeString(*args[i])
		*args[i] = string(vByte)
		if _err != nil {
			return _err
		}
	}

	//当未接实例id时，赋值为空
	if len(args) < 4 {
		args = append(args, tea.String(""))
	}

	client, _err := CreateClient(args[0], args[1])
	if _err != nil {
		return _err
	}

	_, _err = os.Stat("./" + string(*args[2]) + ".txt")
	if _err != nil {
		fmt.Println("元数据不存在")
		nodedatamap := GetNewData(args, client)
		MapToFile("./"+string(*args[2])+".txt", nodedatamap)
	} else {
		fmt.Println("元数据存在")
		nodedatamap := GetNewData(args, client)
		oldnodedatamap := FileToMap("./" + string(*args[2]) + ".txt")
		DataDiff(nodedatamap, oldnodedatamap, "NewToOld")
		DataDiff(oldnodedatamap, nodedatamap, "OldToNew")
		MapToFile("./"+string(*args[2])+".txt", nodedatamap)
	}

	return
}

func main() {
	err := _main(tea.StringSlice(os.Args[1:]))
	HandleError(err, "main")
}
