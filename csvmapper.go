package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/shoobyban/filehelper"
)

var mapCache = map[string]map[string]string{}

func processMapping(newline *map[string]interface{}, key string, line map[string]string, origkey string) error {
	// Example for concatenating data based on multiple columns (multiline):
	// `a.CONCAT |
	// {{template, using .value, .line}}
	// {{some other template}}`
	//
	// Simpler example using template:
	// {{ .value }} or {{ .line.othercolumn }}
	value := line[origkey]
	if strings.HasPrefix(origkey, "CONCAT") {
		i := strings.Index(origkey, "CONCAT")
		ss := strings.Split(origkey, "\n")
		glue := strings.TrimPrefix(ss[0], "CONCAT ")
		sub := []string{}
		for _, item := range ss[1:] {
			val, err := filehelper.Template(item, map[string]interface{}{"value": value, "out": newline, "out_key": origkey[i+3 : len(origkey)-2], "line": line})
			if err != nil {
				j, _ := json.MarshalIndent(line, "", " ")
				fmt.Println(string(j))
				panic(err)
			}
			if val != "" {
				sub = append(sub, val)
			}
		}
		(*newline)[key] = strings.Join(sub, glue)
		return nil
	}
	if strings.Contains(origkey, ".{{") && strings.HasSuffix(origkey, "}}") {
		i := strings.Index(origkey, ".{{")
		k := origkey[:i]
		val, err := filehelper.Template(origkey[i+1:len(origkey)], map[string]interface{}{"value": value, "in_key": k, "out": newline, "out_key": origkey[i+3 : len(origkey)-2], "line": line})
		if err != nil {
			j, _ := json.MarshalIndent(line, "", " ")
			fmt.Println(string(j))
			panic(err)
		}
		(*newline)[key] = val
		return nil
	}
	if strings.HasPrefix(origkey, "MAP(") && strings.HasSuffix(origkey, ")") {
		ss := strings.Split(origkey[4:len(origkey)-1], ":")
		if len(ss) < 2 || len(ss) > 4 {
			j, _ := json.MarshalIndent(line, "", " ")
			fmt.Println(string(j))
			panic("MAP(mapping.csv:key{:separator}{:newseparator}) where mapping.csv is (for example) id,path structure, separator and new separator optional")
		}
		var m map[string]string
		var ok bool
		m, ok = mapCache[ss[0]]
		if !ok {
			m = map[string]string{}
			csvFile, _ := os.Open(ss[0])
			defer csvFile.Close()
			reader := csv.NewReader(bufio.NewReader(csvFile))
			records, err := reader.ReadAll()
			if err != nil {
				j, _ := json.MarshalIndent(line, "", " ")
				fmt.Println(string(j))
				panic(err)
			}
			for _, r := range records {
				m[strings.ToLower(r[1])] = r[0]
			}
			mapCache[ss[0]] = m
		}
		if len(ss) == 2 {
			v, ok := m[line[ss[1]]]
			if ok {
				(*newline)[key] = v
			}
		} else {
			sss := strings.Split(line[ss[1]], ss[2])
			vv := []string{}
			for _, s := range sss {
				v, ok := m[strings.ToLower(s)]
				if ok {
					vv = append(vv, v)
				} else if s != "" {
					fmt.Println("Couldn't find " + s)
				}
			}
			if len(ss) > 3 {
				(*newline)[key] = strings.Join(vv, ss[3])
			} else {
				(*newline)[key] = strings.Join(vv, ss[2])
			}
		}
		return nil
	}
	if strings.HasPrefix(origkey, "LS(") && strings.HasSuffix(origkey, ")") {
		ss := strings.Split(origkey[3:len(origkey)-1], ":")
		if len(ss) != 3 {
			panic("LS(folder:{{ template }}:separator)")
		}
		glob, err := filehelper.Template(ss[1], map[string]interface{}{"value": value, "in_key": key, "out": newline, "out_key": ss[0], "line": line})
		if err != nil {
			panic(err)
		}
		files, err := filepath.Glob(filepath.Join(ss[0], glob))
		if err != nil {
			panic(err)
		}

		(*newline)[key] = strings.Join(files, ss[2])

		return nil
	}
	// default, use key
	(*newline)[key] = value
	return nil
}

func batchLines(lines []map[string]string, mainkey, separator string) (ret []map[string]string) {
	last := -1
	for _, line := range lines {
		if line[mainkey] != "" {
			ret = append(ret, line)
			last++
			for k, val := range line {
				ret[last][k+"_BATCHED"] = ret[last][k+"_BATCHED"] + separator + val
			}
		} else {
			for k, val := range line {
				ret[last][k+"_BATCHED"] = ret[last][k+"_BATCHED"] + separator + val
				if val != "" {
					ret[last][k] = ret[last][k] + separator + val
				}
			}
		}
	}
	return
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: csvmapper [mapping_config.csv] [from.csv] [to.csv] {separator}")
		os.Exit(0)
	}
	mm, mh, err := filehelper.ReadCSV(os.Args[1])

	order := []string{}
	if err != nil {
		panic(err)
	}
	if mh[0] != "to" && mh[1] != "from" && mh[2] != "flags" {
		panic("First argument is not a mapping CSV")
	}
	mapping := map[string]string{}
	flags := map[string]string{}
	for _, m := range mm {
		order = append(order, m["to"])
		mapping[m["to"]] = m["from"]
		flags[m["to"]] = m["flags"]
	}

	csvdata, _, err := filehelper.ReadCSV(os.Args[2])

	if err != nil {
		panic(err)
	}

	//	fmt.Printf("%v", csvdata)

	csvdata = batchLines(csvdata, "sku", "ƒ")

	if err != nil {
		panic(err)
	}

	r := len(csvdata)
	if r == 0 {
		panic("Empty input file")
	}
	c := make(chan map[string]interface{})

	for ln, line := range csvdata {
		go func(line map[string]string, c chan map[string]interface{}, ln int) {
			newline := map[string]interface{}{}
			for key, origkey := range mapping {
				if strings.Contains(line[origkey], "ƒ") && !strings.HasSuffix(key, "}}") {
					for _, part := range strings.Split(line[origkey], "ƒ") {
						copy := make(map[string]string)
						for key, value := range line {
							copy[key] = value
						}
						copy[origkey] = part
						err := processMapping(&newline, key, copy, origkey)
						if err != nil {
							panic(fmt.Sprintf("line %d: %v", ln, err))
						}
					}
				} else {
					err := processMapping(&newline, key, line, origkey)
					if err != nil {
						panic(fmt.Sprintf("line %d: %v", ln, err))
					}
				}
			}
			c <- newline
		}(line, c, ln)
	}

	out := []map[string]interface{}{}
	for {
		if r > 0 {
			select {
			case o := <-c:
				if len(o) > 0 {
					ok := false
					for k, v := range o {
						if v != "" {
							ok = true
						} else if flags[k] == "required" {
							ok = false
							break
						}
					}
					if ok {
						out = append(out, o)
					}
				}
			}
			r = r - 1
		} else {
			break
		}
	}

	file, err := os.Create(os.Args[3])

	if err != nil {
		panic(err)
	}
	fmt.Printf("Finished processing %d lines\nHeaders: %v\n", len(out), order)

	w := csv.NewWriter(file)
	if len(os.Args) > 4 && os.Args[4] != "" {
		w.Comma = rune(os.Args[4][0])
	} else {
		w.Comma = ';'
	}
	err = filehelper.OnlyWriteCSV(*w, order, out)
	if err != nil {
		panic(err)
	}
}
