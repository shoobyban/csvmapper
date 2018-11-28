# CSV Mapper
Transform/map CSV file into a different structure CSV using simple CSV based mapping and go templates

## Usage
```sh
$ csvmapper mapping.csv from.csv to.csv
```

Mapping file has a simple structure

|to|from|flags|
|--|----|-----|
|field1|other file's field 2|required|
|field2|other file's field 1| |

Required will make the row ignored if the field has no value in it.
Also, if `merge lines` option is set the fields having no required field will be count as an extension for the previous row:

|to|from|flags|
|--|----|-----|
|A|a|required|
|B|b| |
|C|c| |

...with csv input:

|a|b|c|
|-|-|-|
|foo1|bar1|bazz|
| | bar2|bazz2|

will result:

|A|B|C|
|-|-|-|
|foo1|bar1ƒbar2|bazzƒbazz2|

Where ƒ is the default separator, can be set with `separator` option.

## Extra possibilities

### Go Template

You can use go templates:

|to|from|flags|
|--|----|-----|
|name|productname.{{ printf "%-10s" .value  }}| |
|price|p1.{{if .line.p2}}{{.line.p2}}{{else}}{{.line.p1}}{{end}}||

Templates have to start with field name (even if .value is not used in the template), then `.{{` and has to finish with `}}`.

Possible values:
|name|description|
|-|-|
|.value|Value from field identified by name before first `.{{` |
|.line|The whole line, can be used like `{{.line.other_field}}`|
|.in_key|Header value of the input file|
|.out_key|Header value of the output file|

## Concatenation 

|to|from|flags|
|--|----|-----|
|vars|CONCAT \| \ | |
|    | A:{{ .line.a }} \ | |
|    | B:{{ .line.b }} \ | |

Will result 

|...|vars|...|
|--|--|--|
|...|A:foo\|B:bar|...|

