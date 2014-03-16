package main

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/codegangsta/martini"
	"gopkg.in/v1/yaml"
)

type Sample struct {
	id     [10]byte
	device [10]byte
	time   time.Time
	per    int16
	values []int16
}

var CONFIG_FN = "./seder.config"

type Config struct {
	DataDir string `yaml:"data_dir"`
}

func main() {
	config, err := ReadConfig(CONFIG_FN)
	if err != nil {
		log.Fatal(err)
	}

	m := martini.Classic()

	m.Get("/", func(req *http.Request) string {
		//		for k, v := range req.Header {
		//			log.Printf("%20s: %s\n", k, v)
		//		}
		return "Hello seder!"
	})

	m.Post("/v0/data", func(res http.ResponseWriter, req *http.Request) []byte {
		body, err := ioutil.ReadAll(req.Body)
		PanicIf(err)
		log.Printf("received %d bytes\n", len(body))
		if len(body) == 0 {
			log.Printf("WARNING: body is empty!")
			for k, v := range req.Header {
				log.Printf("%20s: %s\n", k, v)
			}
			return []byte("!\n")
		}
		samples := decodeV0(body)
		// for _, v := range samples {

		// 	// Format values.
		// 	var buf bytes.Buffer
		// 	for k, v := range v.values {
		// 		buf.WriteString(fmt.Sprintf("s%d: %d, ", k, v))
		// 	}
		// 	log.Printf("t: %30s, id: %s, device: %s, per: %d, nmeas: %d, %s", v.time.UTC(), string(v.id[:]), string(v.device[:]), v.per, len(v.values), buf.String())
		// }
		err = writeToFileV0(samples, config.DataDir)
		if err != nil {
			log.Printf("error writing samples to file: %s", err)
		}
		return []byte("!\n")
	})

	m.Run()
}

// Header description. V0
// TYPE      BYTES     EXAMPLE      DESCRIPTION
// char[10]   10       ABCDEFGHIJ   account id
// char[10]   10       aBK23o#OP@   device id
// u long      4       1394413060   base unix time in seconds (t1)
// u long      4       57326        delta time to be added to base time in milliseconds (t2)
// short       2       800          sample period in milliseconds
// short       2       5            num samples per measurement
// byte        1       2            num measurements (num analog sensors)
// TOTAL      23 bytes
//
// The data is organized as follows:
// SAMPLE    SENSOR    VALUE
// 0         0         123
// 0         1         23
// 1         0         112
// 1         1         25
// ...
// values are of type short (a short stores a 16-bit (2-byte) value. This yields a range of -32,768 to 32,767)
// note that the size of int varies for different ATNEL processors and short is always 2 bytes.
//
// Sample times can be decoded to millisecs as follows:
// t1 => lastPolledTime (secs)
// t2 => deltaTime (millisecs)
// SAMPLE   TIME
// 0        t1 * 1000 + t2
// 1        t1 * 1000 + t2 + 1 * SAMP_PERIOD
// 2        t1 * 1000 + t2 + 2 * SAMP_PERIOD
// ...
func decodeV0(body []byte) (samples []*Sample) {

	var id [10]byte
	var device [10]byte
	var t1, t2 uint32
	var per, nsamp int16
	var nmeas byte

	buf := bytes.NewReader(body)
	err := binary.Read(buf, binary.LittleEndian, &id)
	PanicIf(err)
	//log.Printf("id: %s", string(id[:]))

	err = binary.Read(buf, binary.LittleEndian, &device)
	PanicIf(err)
	//log.Printf("device: %s", string(device[:]))

	err = binary.Read(buf, binary.LittleEndian, &t1)
	PanicIf(err)
	//log.Printf("t1: %d", t1)

	err = binary.Read(buf, binary.LittleEndian, &t2)
	PanicIf(err)
	//log.Printf("t2: %d", t2)

	err = binary.Read(buf, binary.LittleEndian, &per)
	PanicIf(err)
	//log.Printf("per: %d", per)

	err = binary.Read(buf, binary.LittleEndian, &nsamp)
	PanicIf(err)
	//log.Printf("nsamp: %d", nsamp)

	err = binary.Read(buf, binary.LittleEndian, &nmeas)
	PanicIf(err)
	//log.Printf("nmeas: %d", nmeas)

	samples = make([]*Sample, nsamp, nsamp)
	var i16, i int16
	var j byte
	for i = 0; i < nsamp; i++ {
		samples[i] = &Sample{
			id:     id,
			device: device,
			time:   time.Unix(int64(t1), (int64(t2)+int64(per*i))*1000000),
			per:    per,
		}
		values := make([]int16, nmeas, nmeas)
		for j = 0; j < nmeas; j++ {
			err = binary.Read(buf, binary.LittleEndian, &i16)
			PanicIf(err)
			values[j] = i16
		}
		samples[i].values = values
	}
	return
}

func (sample Sample) row() []string {

	st := make([]string, 0, 20)
	st = append(st, string(sample.id[:]))
	st = append(st, string(sample.device[:]))
	st = append(st, sample.time.UTC().Format(time.RFC3339Nano))
	st = append(st, strconv.Itoa(int(sample.per)))
	for _, v := range sample.values {
		st = append(st, strconv.Itoa(int(v)))
	}
	return st
}

func (sample Sample) header() []string {
	h := make([]string, 0, 20)
	h = append(h, "account")
	h = append(h, "device")
	h = append(h, "time")
	h = append(h, "period")
	for k, _ := range sample.values {
		h = append(h, fmt.Sprintf("A%d", k))
	}
	return h
}

func writeToFileV0(samples []*Sample, dir string) (err error) {

	if len(samples) == 0 {
		return nil
	}

	// Create path.
	t := samples[0].time.UTC()
	fn := fmt.Sprintf("%02d-%s.dat", t.Hour(), string(samples[0].device[:]))
	path := filepath.Join(dir, string(samples[0].id[:]), t.Format("2006/01/02"), fn)
	err = os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		log.Printf("can't create path %s", filepath.Dir(path))
		return
	}

	var file *os.File
	file, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0755)
	if err != nil {
		log.Printf("can't create file %s", path)
		return
	}
	defer file.Close()

	// Write CSV
	writer := csv.NewWriter(file)
	fi, err := file.Stat()
	if err != nil {
		return
	}
	if fi.Size() == 0 {
		// Write header for new files.
		err = writer.Write(samples[0].header())
	}
	if err != nil {
		return
	}
	for _, v := range samples {
		err = writer.Write(v.row())
		if err != nil {
			return
		}
	}
	writer.Flush()
	return nil
}

func PanicIf(err error) {
	if err != nil {
		panic(err)
	}
}

// Read configuration file.
func ReadConfig(filename string) (config *Config, err error) {

	var data []byte
	data, err = ioutil.ReadFile(filename)
	if err != nil {
		return
	}
	config = &Config{}
	err = yaml.Unmarshal(data, config)
	if err != nil {
		return
	}
	log.Printf("config:\n%s\n", config)
	return
}

func (c *Config) String() string {

	d, err := yaml.Marshal(c)
	if err != nil {
		log.Fatal(err)
	}
	return string(d)
}
