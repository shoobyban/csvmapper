package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/shoobyban/filehelper"
)

func processMapping(newline *map[string]interface{}, origkey string, line map[string]string, key string) error {
	// a.CONCAT#Separator#LabelAndInnerSeparator
	// example: in case LabelAndInnerSeparator is 'Label=' and Separator ','
	// a's value will be Label=value1,Label2=value2
	// the # can be replaced by any character that's not in Separator or LabelAndInnerSeparator
	if strings.Contains(key, ".CONCAT") {
		i := strings.Index(key, ".CONCAT")
		ss := strings.Split(key, key[i+7:i+8])
		k := key[:i] // first part is the real key
		//		fmt.Printf("'%s': %d %v '%s'\n", key, i, ss, k)
		if val, ok := (*newline)[k]; ok && val != "" {
			// already existing value + first separator + second part + value
			if line[origkey] == "" {
				return nil
			}
			(*newline)[k] = val.(string) + ss[1] + ss[2] + line[origkey]
			return nil
		}
		// second part + value
		if line[origkey] == "" {
			(*newline)[k] = ""
			return nil
		}
		(*newline)[k] = ss[2] + line[origkey]
		return nil
	}
	if strings.Contains(key, ".{{") && strings.HasSuffix(key, "}}") {
		i := strings.Index(key, ".{{")
		k := key[:i]
		val, err := filehelper.Template(key[i+1:len(key)], map[string]interface{}{"orig": line[origkey], "inkey": origkey, "out": newline, "in": line, "outkey": key[i+3 : len(key)-2]})
		if err != nil {
			panic(err)
		}
		(*newline)[k] = val
		return nil
	}
	// (a,b) => copy it to both fields
	if strings.HasPrefix(key, "(") && strings.HasSuffix(key, ")") && strings.Contains(key, ",") {
		list := strings.Split(key[1:len(key)-1], ",")
		for _, k := range list {
			(*newline)[k] = line[origkey]
		}
		return nil
	}
	// default, use key
	(*newline)[key] = line[origkey]
	return nil
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: csvmapper [mapping_config.csv] [from.csv] [to.csv]")
		os.Exit(0)
	}
	mm, mh, err := filehelper.ReadCSV(os.Args[1])
	if err != nil {
		panic(err)
	}
	if mh[0] != "from" && mh[1] != "to" {
		panic("First argument is not a mapping CSV")
	}
	mapping := map[string]string{}
	flags := map[string]string{}
	for _, m := range mm {
		if m["to"] != "" {
			mapping[m["from"]] = m["to"]
		}
		if strings.HasPrefix(m["to"], "(") && strings.HasSuffix(m["to"], ")") && strings.Contains(m["to"], ",") {
			list := strings.Split(m["to"][1:len(m["to"])-1], ",")
			for _, k := range list {
				flags[k] = m["flags"]
			}
		} else {
			flags[m["to"]] = m["flags"]
		}
	}

	csvdata, head, err := filehelper.ReadCSV(os.Args[2])
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
			for _, origkey := range head {
				if key, ok := mapping[origkey]; ok {
					err := processMapping(&newline, origkey, line, key)
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

	headers := []string{}
	for k := range out[0] {
		headers = append(headers, k)
	}
	file, err := os.Create(os.Args[3])
	if err != nil {
		panic(err)
	}
	fmt.Printf("Finished processing %d lines\nHeaders: %v\n", len(out), headers)
	w := csv.NewWriter(file)
	w.Comma = ';'
	err = filehelper.OnlyWriteCSV(*w, headers, out)
	if err != nil {
		panic(err)
	}
}
