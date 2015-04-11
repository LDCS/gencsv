// gencsv generates a package specific to the hcsv file format described in its input.
//
// An hcsv file is a csv file that
//   (1) uses comma as separator
//   (2) has a header row containing unique non-blank column names
//   (3) has the same number of commas in each row
//   (4) has cell values, that are valid when trimmed of whitespace
//
// The generated package defines a struct corresponding to one row of the target format.
// It has functions to read/write files with the target format to/from in-memory arrays.
// The in-memory array is indexed using possibly multiple, possibly multicolumn indexes.
// One may also define additional "hidden" columns which are useful during processing.
// One may also define "instance" variables which are at the file level.
//
// Once familiar with the procedure, new formats can be added to workflow within minutes.
//
// Gencsv could be useful to an entity involved in dynamic ETL data processing involving csv files of varied formats.
// These typically arise from engagement with several counterparties.
// As an example, consider financial fund with largely automated processes which must engage with new counterparties.
// In this example, counterparties include multiple prime brokers, execution brokers, funds administrator, data vendors, reporting entities etc.
//
// Gencsv does not use reflection anywhere.
// Instead package generation runs off a spec file that lists properties of each column of the format.
// For each column named (say FOO) in the target format, the spec file should contain a "spec row" for FOO.
// This row specifies properties of column FOO (its member name, display name, type, indexes, visibility).
// Each hidden column (say BAR) would require "spec row" for BAR.
//
// Hidden columns are initialized to nil when target format files are read, and not written when target files are written.
// (Unless the corresponding *Hidden funcs are called).
// The column names that are written out can be specified with the --HeaderStyle commandline parameter.
// (1) internal - the golang-legal names of the corresponding in-memory struct members
// (1) external - taken from the "headerstring" column of the spec file
//
// Gencsv can be called in 2 modes
//   (1) GENCFG: to generate the spec file
//   (2) GENCSV: to generate the package file (from the spec file)
//
// A spec file created in GENCFG mode must be hand-edited before it can be used in GENCSV mode.
// At minimum, you must set one favourite index
// Simply putting "*index" in the "hasindex" column of spec row FOO will generate a single-column index on target format column FOO.
//
// But if there is even one multi-column index, you will have to specify ALL indexes in the longhand form.
// In longhand form the hasindex column describes how column FOO participates in each index, BAZ, where it participates.
// The string "index(BAZ=N=/)" in the hasindex column of spec row FOO specifies that column FOO is part of the multi-column index named BAZ.
//   Further, it is in (0-indexed) position N and the key is formed by joining component key values with separator "/".
//   If preceded with "*" BAZ is noted to be the favourite index, i.e, the order to be used when writing out the file in sorted order.
// For participation in a number of indexes, just concatenate index descriptions.
//
// The package file which is created should not be hand edited.
// Often, you will decide you want change the number or components of the indexes.
// To do so, just change the spec file, then rerun gencsv.
//
// Conventions:
// (1) Generate all such packages in subdirs of the "anydset" directory.
// (2) Add custom functionality in the file <packagename>_more.go in the same directory as the generated file.
//
// Gencsv also generates test "main program" and a bash script to run the main program.
// If "--Underscore" commandline parameter is "end", gencsv appends an underscore to column members.
//
// Instance variables that are of foo.Bar type will result in an import of "foo" in the generated code
//
// Gencsv generates code to store multiple hcsv instances in a map (PointerMap)
// If an instance variable's config has "sort" in its hasindex field, code is generated to sort the PointerMap by that variable
// (If the type is time.Time, it is sorted by UnixNano())

package main

import (
	"fmt"
	"github.com/LDCS/genutil"
	"github.com/LDCS/sflag"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

var (
	opt = struct {
		Usage       string "generate bespoke package for an hcsv format"
		Pkg         string "Name of the hcsv format, to be used as its package name"
		CapsPkg     string "If left empty, it will be set ToUpper(Pkg)				|"
		Cfg         string "Valid hcsv file holding spec of the target hcsv format	|"
		Ofile       string "Filename for the generated package file"
		TestMain    string "Filename of main program for testing					| ./TestMain.go"
		TestBash    string "Filename for the bash script for testing				| ./TestMain.bash"
		HeaderStyle string "Member variable names should be internal or external	| internal"
		Underscore  string "Members should have (no or end) underscore				| no"
		Gopath      string "GOPATH for the test program"
		Goroot      string "GOROOT for the test program"
		Args        []string
	}{}
	capsName         = ""
	endUnder         = ""
	needStrConv      = false
	needBytes        = false
	needDropRowInt64 = false
)

func parseArgs() bool {
	sflag.Parse(&opt)
	if len(opt.Cfg) < 1 {
		return false
	} // Assume it is being called in gencfg mode

	if len(opt.Args) < 0 {
		fmt.Println("Usage: go-gencsv filename ofile")
		panic("filename not specified")
	}
	capsName = opt.CapsPkg
	if len(capsName) < 1 {
		capsName = strings.ToUpper(opt.Pkg)
	}
	if len(opt.Pkg) < 1 {
		panic("Pkg must be specified\n" + opt.Usage)
	}
	switch opt.HeaderStyle {
	case "internal", "external":
	default:
		panic("HeaderStyle must be \"internal\" or \"external\", not " + opt.HeaderStyle + "\n" + opt.Usage)
	}
	switch opt.Underscore {
	case "end":
		endUnder = "_"
	case "no":
		endUnder = ""
	default:
		panic("Underscore must be one of \"end\" or \"no\", not " + opt.Underscore + "\n" + opt.Usage)
	}
	return true
}

type xatt struct {
	xname string
	xtype string
}

// GENCSVElem describes a row of the spec file, in addition to some non-spec helper variables
type GENCSVElem struct {
	Name         string
	Headerstring string
	Type         string
	OutType      string
	Hasindex     string
	Finaltype    string
	Hidden       bool
	Header       bool
	Footer       bool
	FooterCount  bool
	LastShown    bool
	Last         bool
	FirstShown   bool
	Xarr         []xatt
	Yarr         []xatt
}

// GENCSVElemPtr is shorthand
type GENCSVElemPtr *GENCSVElem

// GENCSVElemPtrSlice is shorthand
type GENCSVElemPtrSlice []GENCSVElemPtr

type bslice []byte

var comma byte = ','
var fslash byte = '/'
var arr GENCSVElemPtrSlice
var yarr GENCSVElemPtrSlice

func loadElem(_bsl bslice) (row *GENCSVElem) {
	lenslice, ii, jj, print := len(_bsl), 0, 0, false
	row = new(GENCSVElem)
	row.Hidden = false
	row.Header = false
	row.Footer = false
	row.FooterCount = false
	row.LastShown = false
	row.Last = false
	row.FirstShown = false
	for ii = jj; jj < lenslice; jj++ {
		if _bsl[jj] == comma {
			row.Name = strings.TrimSpace(string(_bsl[ii:jj]))
			if print {
				fmt.Println("Name=", row.Name)
			}
			jj++
			break
		}
	}
	for ii = jj; jj < lenslice; jj++ {
		if _bsl[jj] == comma {
			row.Headerstring = strings.TrimSpace(string(_bsl[ii:jj]))
			if print {
				fmt.Println("Headerstring=", row.Headerstring)
			}
			jj++
			break
		}
	}
	for ii = jj; jj < lenslice; jj++ {
		if _bsl[jj] == comma {
			row.Type = strings.TrimSpace(string(_bsl[ii:jj]))
			if row.Type == "" {
				row.Type = "string"
			}
			if print {
				fmt.Println("Type=", row.Type)
			}
			jj++
			break
		}
	}
	for ii = jj; jj < lenslice; jj++ {
		if _bsl[jj] == comma {
			row.Hasindex = strings.TrimSpace(string(_bsl[ii:jj]))
			if row.Hasindex == "" {
				row.Hasindex = "noindex"
			}
			if print {
				fmt.Println("Hasindex=", row.Hasindex)
			}
			jj++
			break
		}
	}
	for ii = jj; jj < lenslice; jj++ {
		if jj+1 == lenslice {
			if _bsl[jj-1] == '' {
				jj--
			}
			row.Finaltype = strings.TrimSpace(string(_bsl[ii:jj]))
			if row.Finaltype == "" {
				row.Finaltype = "none"
			}
			if print {
				fmt.Println("Finaltype=", row.Finaltype)
			}
			jj++
			break
		}
	}

	mightNeedBytes := false
	row.OutType = row.Type
	switch row.Type {
	case "int64", "bool":
		needStrConv = true
	case "float64":
		needStrConv = true
		mightNeedBytes = true
	case "yyyymmdd":
		needStrConv = true
		row.OutType = "int64"
	case "yyyy_mm_dd":
		needStrConv = true
		row.OutType = "int64"
		mightNeedBytes = true
	case "YYYY_MM_DD_HH_MM_SS_mmm_zz":
		needStrConv = true
		row.OutType = "int64"
		mightNeedBytes = true
	}

	perinstance := false
	switch row.Finaltype {
	case "", "none":
	case "instance":
		perinstance = true
	default:
		pairs := strings.Split(row.Finaltype, "/")
		for _, vv := range pairs {
			kvs := strings.Split(vv, ":")
			if (len(kvs) == 1) && (kvs[0] == "hidden") {
				row.Hidden = true
				continue
			}
			if (len(kvs) >= 1) && (kvs[0] == "header") {
				row.Hidden = true
				row.Header = true
				mightNeedBytes = false
				continue
			}
			if (len(kvs) >= 1) && (kvs[0] == "footer") {
				row.Hidden = true
				row.Footer = true
				mightNeedBytes = false
				if (len(kvs) > 1) && (kvs[1] == "rowcount") {
					row.FooterCount = true
				}
				continue
			}
			xrow := new(xatt)
			xrow.xname = row.Name + "_" + strings.Trim(kvs[0], "\t\n\r ") + "_"
			xrow.xtype = strings.Trim(kvs[1], "\t\n\r ")
			row.Xarr = append(row.Xarr, *xrow)
		}
	}
	if mightNeedBytes {
		needBytes = true
	}

	switch perinstance {
	case true:
		_, ok := yAddRow(row)
		if !ok {
			fmt.Println("gencsv bad per-instance variable row=", string(_bsl))
		}
	case false:
		_, ok := addRow(row)
		if !ok {
			fmt.Println("gencsv bad row=", string(_bsl))
		}
	}
	return row
}

func addRow(_row *GENCSVElem) (*GENCSVElem, bool) {
	ok := true
	arr = append(arr, _row)
	return _row, ok
}

func yAddRow(_row *GENCSVElem) (*GENCSVElem, bool) {
	ok := true
	yarr = append(yarr, _row)
	return _row, ok
}

func loadSpec(_fname string) {
	rr := genutil.OpenAny(_fname)
	numread, numbad, numempty, numcomment := 0, 0, 0, 0
	for first := true; ; first = false {
		bsl, err := rr.ReadSlice('\n')
		if err != nil && err != io.EOF {
			panic(err)
		}
		if err == io.EOF {
			break
		}
		if len(bsl) < 2 {
			numempty++
			continue
		}
		if bsl[0] == '#' {
			numcomment++
			continue
		}
		if len(bsl) < 3 {
			numbad++
			continue
		}
		if first {
			matchline := "name,headerstring,type,hasindex,finaltype"
			if string(bsl)[:len(matchline)] == matchline {
			}
		}
		if (bsl[0] == 'n') && (bsl[1] == 'a') && (bsl[2] == 'm') && (bsl[3] == 'e') && (bsl[4] == ',') {
			if !first {
				numbad++
			}
			continue
		}
		if !first {
			loadElem(bsl)
			numread++
		}
	}

	for ii := len(arr) - 1; ii >= 0; ii-- {
		if !arr[ii].Hidden {
			arr[ii].LastShown = true
			break
		}
	} // Note that .Footer implies .Hidden, so Footer will not be shown
	for ii := len(arr) - 1; ii >= 0; ii-- {
		if !arr[ii].Footer {
			arr[ii].Last = true
			break
		}
	} // was : arr[len(arr)-1 ].Last = true;
	for ii := 0; ii < len(arr); ii++ {
		if !arr[ii].Hidden {
			arr[ii].FirstShown = true
			break
		}
	}

	fmt.Println(" numread=", numread, "numbad=", numbad, "numempty=", numempty, "numcomment=", numcomment)
}

func writeTest(_fo io.Writer) {
	io.WriteString(_fo, "// Package main is Machine Generated - By gencsv.go - For testing "+opt.Pkg+" - Do not edit\n")
	io.WriteString(_fo, "package main\n")
	io.WriteString(_fo, "\n")
	io.WriteString(_fo, "import (\n")
	io.WriteString(_fo, "    \"fmt\"\n")
	io.WriteString(_fo, "    \"anydset/"+opt.Pkg+"\"\n")
	io.WriteString(_fo, ")\n")
	io.WriteString(_fo, "\n")
	io.WriteString(_fo, "var (\n")
	io.WriteString(_fo, "    "+opt.Pkg+"1 	*"+opt.Pkg+"."+capsName+"\n")
	io.WriteString(_fo, ")\n")
	io.WriteString(_fo, "\n")
	io.WriteString(_fo, "func main() {\n")
	io.WriteString(_fo, "\n")
	io.WriteString(_fo, "    "+opt.Pkg+"1	= "+opt.Pkg+".New"+capsName+"(true)\n")
	io.WriteString(_fo, "    "+opt.Pkg+"1.Load(\"samples/"+opt.Pkg+".csv\")\n")
	io.WriteString(_fo, "    fmt.Println(\"loaded\")\n")
	io.WriteString(_fo, "    "+opt.Pkg+"1.Proc(\"samples/"+opt.Pkg+".csv\", "+opt.Pkg+".ProcFuncSample)\n")
	io.WriteString(_fo, "    fmt.Println(\"processed\")\n")
	io.WriteString(_fo, "    "+opt.Pkg+"1.WriteFile(\"out."+opt.Pkg+".csv\")\n")
	io.WriteString(_fo, "}\n")
}

func writeDoit(_fo io.Writer) {
	io.WriteString(_fo, "#!/bin/bash\n")
	io.WriteString(_fo, "# Machine Generated - By gencsv.go - Do not edit\n")
	if opt.Goroot != "" && opt.Gopath != "" {
		io.WriteString(_fo, "export GOROOT="+opt.Goroot+"\n")
		io.WriteString(_fo, "export GOPATH="+opt.Gopath+"\n")
	}
	io.WriteString(_fo, "\n")
	io.WriteString(_fo, "$GOROOT/bin/go build test_"+opt.Pkg+".go\n")
	io.WriteString(_fo, "if [ \"$?\" = \"0\" ]; then\n")
	io.WriteString(_fo, "  echo ./test_"+opt.Pkg+"\n")
	io.WriteString(_fo, "       ./test_"+opt.Pkg+"\n")
	io.WriteString(_fo, "else\n")
	io.WriteString(_fo, "      echo \"failed "+opt.Pkg+"\"\n")
	io.WriteString(_fo, "fi\n")
}

func writePre(_fo io.Writer) {
	io.WriteString(_fo, "// Package "+opt.Pkg+" was Machine Generated - By gencsv.go - Do not edit - Put your handcrafted code in "+opt.Pkg+"_more.go\n")
	io.WriteString(_fo, "package "+opt.Pkg+"\n")
	io.WriteString(_fo, "\n")
	io.WriteString(_fo, "import (\n")
	io.WriteString(_fo, "	\"fmt\"\n")
	io.WriteString(_fo, "	\"io\"\n")
	io.WriteString(_fo, "	\"log\"\n")
	io.WriteString(_fo, "	\"sort\"\n")
	io.WriteString(_fo, "	\"genutil\"\n")
	if needStrConv {
		io.WriteString(_fo, "	\"strconv\"\n")
	}
	if needBytes {
		io.WriteString(_fo, "	\"bytes\"\n")
	}
	io.WriteString(_fo, "	\"strings\"\n")

	// Now set up imports for packages used by instance variables
	if true {
		ydone := map[string]bool{}
		for _, yrow := range yarr {
			switch yrow.Type {
			case "string", "int", "int64", "float64":
			default:
				if strings.Contains(yrow.Type, ".") {
					ypkg := yrow.Type[:strings.Index(yrow.Type, ".")]
					if !ydone[ypkg] {
						ydone[ypkg] = true
						io.WriteString(_fo, "	\""+ypkg+"\"\n")
					}
				}
			}
		}
	}
	io.WriteString(_fo, "        )\n\n")

	io.WriteString(_fo, "type bslice []byte\n")
	io.WriteString(_fo, "var comma  byte	= ','\n")
	io.WriteString(_fo, "\n")
}

type indexMapElem struct {
	Name string
	Rows []string
	Type string
	Sep  string
}
type indexMapElemPtr *indexMapElem
type indexMapType map[string]indexMapElemPtr

var indexMap indexMapType

var favIM *indexMapElem // Will be the index used in Sortedwrite* funcs
var sortedIndexKeys []string
var sortedIndexVals []indexMapElemPtr

func makeIndexes() {
	favName := "" // This will be used in Sortwrite calls
	indexMap = make(map[string]indexMapElemPtr)
	for _, row := range arr {
		if row.Header || row.Footer {
			continue
		}
		switch row.Hasindex {
		case "noindex", "none", "":
		case "index", "*index": // simple index
			im := new(indexMapElem)
			im.Name = row.Name
			im.Sep = ":"
			im.Rows = append(im.Rows, row.Name)
			im.Type = row.Type
			if im.Type == "int64" {
				needDropRowInt64 = true
			}
			indexMap[row.Name] = im
			if (row.Hasindex == "*index") && (favName == "") {
				favName = row.Name
			}
			fmt.Println(" Creating simple index im.Name=", im.Name, " type=", im.Type, " on column=", im.Name)
		default:
			ix := row.Hasindex
			favthis, favnext := false, false
			if strings.HasPrefix(ix, "*index(") {
				if favName == "" {
					favthis, favnext = true, false
				}
				ix = ix[1:]
			} else if !strings.HasPrefix(row.Hasindex, "index(") {
				panic("makeIndexes: bad index column in row=" + row.Name)
			}
			parts := strings.Split(ix[6:], "index(")
			fmt.Println("makeIndexes hasindex=", row.Hasindex)
			for ipi, ip := range parts {
				if strings.HasSuffix(ip, "*") {
					if favName == "" {
						if !favthis {
							favthis, favnext = false, true
						}
					}
					ip = ip[:len(ip)-1] // discard the * (favindex marker)
				}
				parts2 := strings.SplitN(ip[:len(ip)-1], "=", 3) // drop the trailing ")" before split
				iname, inum := parts2[0], genutil.ToInt(parts2[1], 0)
				fmt.Println("    iname=", iname, "  inum=", inum)
				im, ok := indexMap[iname]
				if favName == "" {
					if favthis {
						favName, favthis, favnext = iname, false, false
					} else if favnext {
						favthis, favnext = true, false // set favthis to be true for the next iter
					}
				}
				switch ok {
				case true: // multipart index, seen a part before
					if int(inum) >= len(im.Rows) {
						fmt.Println(" Appending to found index im.Name=", im.Name, " which pre has len ", len(im.Rows), " sep", im.Sep)
						im.Rows = im.Rows[:int(inum)+1]
					}
					im.Rows[inum] = row.Name
					switch ipi {
					case 0:
						im.Type = row.Type // singlepart keys can be part[0]
					default:
						im.Type = "string" // force multipart into string keys
					}
					if im.Type == "int64" {
						needDropRowInt64 = true
					}
					fmt.Println(" Appending to found index im.Name=", im.Name, " which now has len ", len(im.Rows), " sep", im.Sep)

				default: // not seen this index before
					im = new(indexMapElem)
					im.Name = iname
					if len(parts2) > 2 {
						im.Sep = parts2[2]
					}
					im.Rows = make([]string, inum+1, 1024)
					im.Rows[inum] = row.Name
					switch len(parts2) {
					case 0:
						im.Type = row.Type // singlepart keys can be part[0]
					default:
						im.Type = "string" // force multipart into string keys
					}
					if im.Type == "int64" {
						needDropRowInt64 = true
					}
					indexMap[iname] = im
					fmt.Println(" Creating unfound index im.Name=", im.Name, " which now has len ", len(im.Rows), " sep", im.Sep, "from parts=", parts2)
				}
			}
		}
	}

	sortedIndexKeys = make([]string, len(indexMap))
	ii := 0
	for kk := range indexMap {
		sortedIndexKeys[ii] = kk
		ii++
	}
	sort.Strings(sortedIndexKeys)
	sortedIndexVals = make([]indexMapElemPtr, len(sortedIndexKeys))
	ii = 0
	for ii, kk := range sortedIndexKeys {
		sortedIndexVals[ii] = indexMap[kk]
	}

	if len(sortedIndexVals) <= 0 {
		panic("gencsv.makeIndexes: PanicExit - Please define atleast one index on " + capsName + "\n")
	}

	if favName == "" {
		panic("gencsv.makeIndexes: PanicExit - Please tag one index as favourite (to be used in Sortwrite*\n")
		// favName = sortedIndexVals[0].Name
	}
	favIM, _ = indexMap[favName]
}

func writeStruct(_fo io.Writer) {
	makeIndexes()

	io.WriteString(_fo, "// "+capsName+"Elem describes one row\n")
	io.WriteString(_fo, "//")
	for _, row := range arr {
		if row.Header {
			io.WriteString(_fo, row.Name+",")
		}
	}
	io.WriteString(_fo, "\n")
	io.WriteString(_fo, "//")
	for _, row := range arr {
		if !(row.Header || row.Footer) {
			io.WriteString(_fo, row.Name+",")
		}
	}
	io.WriteString(_fo, "\n")
	io.WriteString(_fo, "//")
	for _, row := range arr {
		if row.Footer {
			io.WriteString(_fo, row.Name+",")
		}
	}
	io.WriteString(_fo, "\n")
	io.WriteString(_fo, "type "+capsName+"Elem struct {\n")
	for _, row := range arr {
		if row.Header || row.Footer {
			continue
		}
		io.WriteString(_fo, "	"+row.Name+endUnder+"	"+row.OutType+"\n")
		for _, xrow := range row.Xarr {
			io.WriteString(_fo, "		"+xrow.xname+"_	"+xrow.xtype+"\n")
		}
		switch row.Type {
		case "YYYY_MM_DD_HH_MM_SS_mmm_zz":
			io.WriteString(_fo, "	"+row.Name+"_hhmmss"+endUnder+"	int64\n")
			io.WriteString(_fo, "	"+row.Name+"_mmm"+endUnder+"	int64\n")
			io.WriteString(_fo, "	"+row.Name+"_zz"+endUnder+"	int64\n")
		}
	}
	io.WriteString(_fo, "}\n\n")

	io.WriteString(_fo, "//"+capsName+"ElemPtr is shorthand\n")
	io.WriteString(_fo, "type	"+capsName+"ElemPtr *"+capsName+"Elem\n")
	io.WriteString(_fo, "//"+capsName+"ElemPtrSlice is shorthand\n")
	io.WriteString(_fo, "type	"+capsName+"ElemPtrSlice []"+capsName+"ElemPtr\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "//"+capsName+" holds the in-memory representation of an instance this target file format\n")
	io.WriteString(_fo, "type "+capsName+" struct{\n")
	io.WriteString(_fo, "	Verbose_ bool\n")
	io.WriteString(_fo, "	Silent_ bool\n")
	io.WriteString(_fo, "	Loadhidden_ bool\n")
	io.WriteString(_fo, "	Nullkey_ bool\n")
	io.WriteString(_fo, "	Numread_ int\n")
	io.WriteString(_fo, "	Numrows_ int\n")
	io.WriteString(_fo, "	LoadedFilename_ string\n")

	// perinstance variables
	for _, row := range yarr {
		io.WriteString(_fo, "	"+row.Name+"_	"+row.OutType+"\n")
	}

	// non-perinstance variables
	for _, row := range arr {
		if !(row.Header || row.Footer) {
			continue
		}
		comment := ""
		switch {
		case row.Header:
			comment = "	// header"
		case row.Footer:
			comment = "	// footer"
		}
		io.WriteString(_fo, "	"+row.Name+endUnder+"	"+row.OutType+comment+"\n")
		for _, xrow := range row.Xarr {
			io.WriteString(_fo, "		"+xrow.xname+"_	"+xrow.xtype+"\n")
		}
		switch row.Type {
		case "YYYY_MM_DD_HH_MM_SS_mmm_zz":
			io.WriteString(_fo, "	"+row.Name+"_hhmmss"+endUnder+"	int64\n")
			io.WriteString(_fo, "	"+row.Name+"_mmm"+endUnder+"	int64\n")
			io.WriteString(_fo, "	"+row.Name+"_zz"+endUnder+"	int64\n")
		}
	}
	for _, row := range sortedIndexVals {
		io.WriteString(_fo, " Map"+row.Name+"2"+capsName+" map["+row.Type+"]"+capsName+"ElemPtrSlice\n")
	}
	io.WriteString(_fo, " }\n")
	io.WriteString(_fo, "\n") //
	// ========================================================
	io.WriteString(_fo, "// New"+capsName+" instantiates an empty instance of target file format"+capsName+"\n")
	io.WriteString(_fo, "func New"+capsName+"(_verbose bool) *"+capsName+" {\n")
	io.WriteString(_fo, "	self	:= new("+capsName+")\n")
	io.WriteString(_fo, "	self.Verbose_    	      = _verbose\n")
	io.WriteString(_fo, "	self.Silent_    	      = false\n")
	io.WriteString(_fo, "	self.Loadhidden_   	      = false\n")
	io.WriteString(_fo, "	self.Nullkey_    	      = true\n")
	for _, row := range sortedIndexVals {
		io.WriteString(_fo, "	self.Map"+row.Name+"2"+capsName+"		= make(map["+row.Type+"]"+capsName+"ElemPtrSlice)\n")
	}
	io.WriteString(_fo, "	return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	// ========================================================
	io.WriteString(_fo, "// Nil returns a nil pointer to "+capsName+"\n")
	io.WriteString(_fo, "func Nil() *"+capsName+" {\n")
	io.WriteString(_fo, "	return (*"+capsName+")(nil)\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	// ========================================================
	io.WriteString(_fo, "// NilElemPtr returns a nil pointer to a row of "+capsName+"\n")
	io.WriteString(_fo, "func NilElemPtr() *"+capsName+"Elem {\n")
	io.WriteString(_fo, "	return (*"+capsName+"Elem)(nil)\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	// ========================================================
	io.WriteString(_fo, "// Arr returns an array of rows of "+capsName+"\n")
	io.WriteString(_fo, "func Arr(_len int) []*"+capsName+"{\n")
	io.WriteString(_fo, "	arr := make([]*"+capsName+", _len)\n")
	io.WriteString(_fo, "	return arr\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	// ========================================================
	io.WriteString(_fo, "// Silent turns off verbosity when subsequently processing this instance of "+capsName+"\n")
	io.WriteString(_fo, "func (self *"+capsName+") Silent() *"+capsName+" {\n")
	io.WriteString(_fo, "	self.Silent_    	      = true\n")
	io.WriteString(_fo, "	return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	io.WriteString(_fo, "// Verbose sets verbosity when subsequently processing this instance of "+capsName+"\n")
	io.WriteString(_fo, "func (self *"+capsName+") Verbose(_verbose bool) *"+capsName+" {\n")
	io.WriteString(_fo, "	self.Silent_    	      = !_verbose\n")
	io.WriteString(_fo, "	return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	io.WriteString(_fo, "// Loadhidden sets the subsequent load to expect and load hidden columns, for this instance of "+capsName+"\n")
	io.WriteString(_fo, "func (self *"+capsName+") Loadhidden() *"+capsName+" {\n")
	io.WriteString(_fo, "	self.Loadhidden_    	      = true\n")
	io.WriteString(_fo, "	return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	io.WriteString(_fo, "// Setloadhidden sets whether the subsequent load should expect and load hidden columns, for this instance of "+capsName+"\n")
	io.WriteString(_fo, "func (self *"+capsName+") Setloadhidden(_loadhidden bool) *"+capsName+" {\n")
	io.WriteString(_fo, "	self.Loadhidden_    	      = _loadhidden\n")
	io.WriteString(_fo, "	return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	io.WriteString(_fo, "// Status prints information about the recent load operation for this instance of "+capsName+"\n")
	io.WriteString(_fo, "func (self *"+capsName+") Status(_verbose bool) *"+capsName+" {\n")
	io.WriteString(_fo, "	if !_verbose { return self }\n")
	io.WriteString(_fo, "	fmt.Println(\"LoadedFilename=\", self.LoadedFilename_, \"Numread=\", self.Numread_, \"Numrows=\", self.Numrows_)\n")
	io.WriteString(_fo, "	return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	io.WriteString(_fo, "// Nullkey sets whether subsequent load will balk at null keys, for this instance of "+capsName+"\n")
	io.WriteString(_fo, "func (self *"+capsName+") Nullkey(_ok bool) *"+capsName+" {\n")
	io.WriteString(_fo, "	self.Nullkey_    	      = _ok\n")
	io.WriteString(_fo, "	return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	// ========================================================
	for _, row := range yarr {
		io.WriteString(_fo, "func (self *"+capsName+") SetInstance"+row.Name+"(_val "+row.Type+") *"+capsName+"{\n")
		io.WriteString(_fo, " self."+row.Name+"_   = _val\n")
		io.WriteString(_fo, "   return self\n")
		io.WriteString(_fo, "}\n")
		io.WriteString(_fo, "\n")
	}
	// ========================================================
	io.WriteString(_fo, "// Clear forgets any previously read rows for this instance of "+capsName+"\n")
	io.WriteString(_fo, "func (self *"+capsName+") Clear() *"+capsName+" {\n")
	for _, row := range sortedIndexVals {
		io.WriteString(_fo, "	self.Map"+row.Name+"2"+capsName+"		= make(map["+row.Type+"]"+capsName+"ElemPtrSlice)\n")
	}
	io.WriteString(_fo, "	self.Numrows_	= 0\n")
	io.WriteString(_fo, "	return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	// ========================================================

	io.WriteString(_fo, "// ShareAllRows shares each row with another "+capsName+" instance\n")
	io.WriteString(_fo, "func (self *"+capsName+") ShareAllRows(_other *"+capsName+") *"+capsName+" {\n")
	io.WriteString(_fo, "     for _, rows := range _other.Map"+favIM.Name+"2"+capsName+" {\n")
	io.WriteString(_fo, "         for _, row := range rows {\n")
	io.WriteString(_fo, "              if _, ok	:= self.AddRow(row); !ok {\n")
	io.WriteString(_fo, "     	          fmt.Println(\""+capsName+": error adding row \"); PrintRowSep(row, \";\", \"\\n\")\n")
	io.WriteString(_fo, "              }\n")
	io.WriteString(_fo, "         }\n")
	io.WriteString(_fo, "     }\n")
	io.WriteString(_fo, "     return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	// ========================================================

	io.WriteString(_fo, "// PrintRows prints each rows in the specified slice\n")
	io.WriteString(_fo, "func (self *"+capsName+") PrintRows(_rows "+capsName+"ElemPtrSlice) {\n")
	io.WriteString(_fo, "     for idx, row := range _rows {\n")
	io.WriteString(_fo, "         fmt.Println(\"=========idx=\", idx, \"====\")\n")
	io.WriteString(_fo, "     	 self.PrintRow(row)\n")
	io.WriteString(_fo, "     }\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// PrintDupeRows prints rows which share a particular index key, if more than one row shares that key\n")
	io.WriteString(_fo, "func (self *"+capsName+") PrintDupeRows(_rows "+capsName+"ElemPtrSlice) {\n")
	io.WriteString(_fo, "     if(len(_rows) < 2) { return }\n")
	io.WriteString(_fo, "     for idx, row := range _rows {\n")
	io.WriteString(_fo, "         fmt.Println(\"=========idx=\", idx, \"====\")\n")
	io.WriteString(_fo, "     	 self.PrintRow(row)\n")
	io.WriteString(_fo, "     }\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	// ========================================================
	io.WriteString(_fo, "// DropRow removes the row (with specified key and position) from each index it participates in, in reorder-UNSAFE manner\n")
	io.WriteString(_fo, "func (self *"+capsName+") DropRow(_key string, _idx int) {\n")
	io.WriteString(_fo, "    var rows "+capsName+"ElemPtrSlice\n")
	io.WriteString(_fo, "    var ok bool\n")
	for _, row := range sortedIndexVals {
		switch row.Type {
		case "string":
			io.WriteString(_fo, "     rows, ok = self.Map"+row.Name+"2"+capsName+"[_key]; if !ok { return }\n")
			io.WriteString(_fo, "     if _idx < len(rows) - 1 { rows[_idx] = rows[len(rows) - 1] }\n")
			io.WriteString(_fo, "     self.Map"+row.Name+"2"+capsName+"[_key] = rows[:len(rows)-1]\n")
		default:
		}
	}
	io.WriteString(_fo, "     self.Numrows_--\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	// ========================================================
	if needDropRowInt64 {
		io.WriteString(_fo, "func (self *"+capsName+") DropRowInt64(_key int64, _idx int) {\n")
		io.WriteString(_fo, "    var rows "+capsName+"ElemPtrSlice\n")
		io.WriteString(_fo, "    var ok bool\n")
		for _, row := range sortedIndexVals {
			switch row.Type {
			case "int64":
				io.WriteString(_fo, "     rows, ok = self.Map"+row.Name+"2"+capsName+"[_key]; if !ok { return }\n")
				io.WriteString(_fo, "     if _idx < len(rows) - 1 { rows[_idx] = rows[len(rows) - 1] }\n")
				io.WriteString(_fo, "     self.Map"+row.Name+"2"+capsName+"[_key] = rows[:len(rows)-1]\n")
			default:
			}
		}
		io.WriteString(_fo, "}\n")
		io.WriteString(_fo, "\n")
	}

	// ========================================================
	io.WriteString(_fo, "// PrintRow prints this row using columns separated by comma and row terminated by newline\n")
	io.WriteString(_fo, "func (self *"+capsName+") PrintRow(_row "+capsName+"ElemPtr) {\n")
	io.WriteString(_fo, "	PrintRowSep(_row, \"\\n\", \"\")\n")
	io.WriteString(_fo, "    }\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// PrintRowSep prints this row using specified inter-column and end-of-row separator\n")
	io.WriteString(_fo, "func PrintRowSep(_row "+capsName+"ElemPtr, _sep string, _sepEnd string) {\n")
	for _, row := range arr {
		if row.Hidden {
			continue
		}
		io.WriteString(_fo, "	fmt.Print(\""+row.Name+endUnder+"=\", _row."+row.Name+endUnder+");fmt.Print(_sep)\n")
	}
	io.WriteString(_fo, "	fmt.Print(_sepEnd)\n")
	io.WriteString(_fo, "    }\n")
	io.WriteString(_fo, "\n")
	// ========================================================
	io.WriteString(_fo, "// SprintRowSep prints this row to a string, using specified inter-column and end-of-row separator\n")
	io.WriteString(_fo, "func SprintRowSep(_row "+capsName+"ElemPtr, _sep string, _sepEnd string) string {\n")
	io.WriteString(_fo, " str	:= \"\"\n")
	for _, row := range arr {
		if row.Hidden {
			continue
		}
		io.WriteString(_fo, "	str += fmt.Sprint(\""+row.Name+endUnder+"=\", _row."+row.Name+endUnder+"); str += fmt.Sprint(_sep)\n")
	}
	io.WriteString(_fo, "	str += fmt.Sprint(_sepEnd)\n")
	io.WriteString(_fo, "   return str\n")
	io.WriteString(_fo, "    }\n")
	io.WriteString(_fo, "\n")
	// ========================================================
	io.WriteString(_fo, "// loadElem loads one row of the file\n")
	io.WriteString(_fo, "func (self *"+capsName+") loadElem(_bsl bslice) (row *"+capsName+"Elem) {\n")
	io.WriteString(_fo, "   lenslice	  := len(_bsl)\n")
	io.WriteString(_fo, "   ii, jj, mm, print := 0, 0, 1, false\n")
	io.WriteString(_fo, "   row   = new("+capsName+"Elem)\n")
	ctrlMCheck := " false            "
	for _, row := range arr {
		if row.Header || row.Footer {
			continue
		}
		if row.Type == "" {
			row.Type = "string"
		} // default empty type to string
		if (!row.LastShown) && (!row.Last) {
			switch row.Type {
			case "string":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = strings.TrimSpace(string(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "bool":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.ToBool(strings.TrimSpace(string(_bsl[ii:jj])),false); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "int64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),0); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyymmdd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),19000101); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyy_mm_dd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.YYYY_MM_DD2yyyymmdd(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "YYYY_MM_DD_HH_MM_SS_mmm_zz":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+", row."+row.Name+"_hhmmss"+endUnder+", row."+row.Name+"_mmm"+endUnder+", row."+row.Name+"_zz"+endUnder+" = genutil.YYYY_MM_DD_HH_MM_SS_mmm_zz2yyyymmdd_hhmmss_mmm_zz(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+"_); }; jj +=mm; break; } }\n")
			case "float64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.ToFloat(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			default:
				panic("unhandled Type_ of field=" + row.Type)
			}
			ctrlMCheck = "_bsl[jj-1] == ''"
		} else if row.LastShown && row.Last { // (LastShown == Last) implies no hidden columns
			switch row.Type {
			case "string":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = strings.TrimSpace(string(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "bool":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToBool(strings.TrimSpace(string(_bsl[ii:jj])),false); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "int64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),0); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyymmdd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),19000101); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyy_mm_dd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.YYYY_MM_DD2yyyymmdd(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "YYYY_MM_DD_HH_MM_SS_mmm_zz":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+", row."+row.Name+"_hhmmss"+endUnder+", row."+row.Name+"_mmm"+endUnder+", row."+row.Name+"_zz"+endUnder+" = genutil.YYYY_MM_DD_HH_MM_SS_mmm_zz2yyyymmdd_hhmmss_mmm_zz(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "float64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToFloat(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			default:
				panic("unhandled Type_ of field=" + row.Type)
			}
			ctrlMCheck = "_bsl[jj-1] == ''"
		} else if row.LastShown { // (LastShown != Last) implies that hidden columns follow
			io.WriteString(_fo, "if !self.Loadhidden_ {\n")
			switch row.Type {
			case "string":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = strings.TrimSpace(string(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "bool":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToBool(strings.TrimSpace(string(_bsl[ii:jj])),false); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "int64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),0); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyymmdd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),19000101); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyy_mm_dd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.YYYY_MM_DD2yyyymmdd(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "YYYY_MM_DD_HH_MM_SS_mmm_zz":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+", row."+row.Name+"_hhmmss"+endUnder+", row."+row.Name+"_mmm"+endUnder+", row."+row.Name+"_zz"+endUnder+" = genutil.YYYY_MM_DD_HH_MM_SS_mmm_zz2yyyymmdd_hhmmss_mmm_zz(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "float64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToFloat(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			default:
				panic("unhandled Type_ of field=" + row.Type)
			}
			io.WriteString(_fo, "} else {\n")
			switch row.Type {
			case "string":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = strings.TrimSpace(string(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "bool":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.ToBool(strings.TrimSpace(string(_bsl[ii:jj])),false); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "int64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),0); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyymmdd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),19000101); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyy_mm_dd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.YYYY_MM_DD2yyyymmdd(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "YYYY_MM_DD_HH_MM_SS_mmm_zz":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+", row."+row.Name+"_hhmmss"+endUnder+", row."+row.Name+"_mmm"+endUnder+", row."+row.Name+"_zz"+endUnder+" = genutil.YYYY_MM_DD_HH_MM_SS_mmm_zz2yyyymmdd_hhmmss_mmm_zz(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "float64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.ToFloat(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			default:
				panic("unhandled Type_ of field=" + row.Type)
			}
			ctrlMCheck = "_bsl[jj-1] == ''"
		} else if row.Last { // (LastShown != Last) so this is the last hidden column
			switch row.Type {
			case "string":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = strings.TrimSpace(string(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "bool":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToBool(strings.TrimSpace(string(_bsl[ii:jj])),false); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "int64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),0); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyymmdd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),19000101); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyy_mm_dd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.YYYY_MM_DD2yyyymmdd(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "YYYY_MM_DD_HH_MM_SS_mmm_zz":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+", row."+row.Name+"_hhmmss"+endUnder+", row."+row.Name+"_mmm"+endUnder+", row."+row.Name+"_zz"+endUnder+" = genutil.YYYY_MM_DD_HH_MM_SS_mmm_zz2yyyymmdd_hhmmss_mmm_zz(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "float64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToFloat(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			default:
				panic("unhandled Type_ of field=" + row.Type)
			}
			io.WriteString(_fo, "}\n")
			ctrlMCheck = "_bsl[jj-1] == ''"
		}
	}
	io.WriteString(_fo, "   _, ok := self.AddRow(row)\n")
	io.WriteString(_fo, "   if !ok { fmt.Println(\""+opt.Pkg+" bad row=\", string(_bsl)) }\n")
	io.WriteString(_fo, "   return row\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// ProcRowFunc"+capsName+" processes one row of the file, useful for processing very large files in streaming manner\n")
	io.WriteString(_fo, "type ProcRowFunc"+capsName+" func(_row *"+capsName+"Elem) bool\n")

	io.WriteString(_fo, "func (self *"+capsName+") procElem(_bsl bslice, _procRowFunc ProcRowFunc"+capsName+") (row *"+capsName+"Elem) {\n")
	io.WriteString(_fo, "   lenslice	 := len(_bsl)\n")
	io.WriteString(_fo, "   ii, jj, mm, print := 0, 0, 1, false\n")
	io.WriteString(_fo, "   row   = new("+capsName+"Elem)\n")
	ctrlMCheck = " false            "
	for _, row := range arr {
		if row.Header || row.Footer {
			continue
		}
		if (!row.LastShown) && (!row.Last) {
			switch row.Type {
			case "string":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = strings.TrimSpace(string(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "bool":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.ToBool(strings.TrimSpace(string(_bsl[ii:jj])),false); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "int64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),0); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyymmdd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),19000101); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyy_mm_dd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.YYYY_MM_DD2yyyymmdd(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "YYYY_MM_DD_HH_MM_SS_mmm_zz":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+", row."+row.Name+"_hhmmss"+endUnder+", row."+row.Name+"_mmm"+endUnder+", row."+row.Name+"_zz"+endUnder+" = genutil.YYYY_MM_DD_HH_MM_SS_mmm_zz2yyyymmdd_hhmmss_mmm_zz(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "float64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.ToFloat(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			default:
				panic("unhandled Type_ of field=" + row.Type)
			}
			ctrlMCheck = "_bsl[jj-1] == ''"
		} else if row.LastShown && row.Last { // (LastShown == Last) implies no hidden columns
			switch row.Type {
			case "string":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = strings.TrimSpace(string(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "bool":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToBool(strings.TrimSpace(string(_bsl[ii:jj])),false); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "int64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),0); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyymmdd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),19000101); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyy_mm_dd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.YYYY_MM_DD2yyyymmdd(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "YYYY_MM_DD_HH_MM_SS_mmm_zz":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+", row."+row.Name+"_hhmmss"+endUnder+", row."+row.Name+"_mmm"+endUnder+", row."+row.Name+"_zz"+endUnder+" = genutil.YYYY_MM_DD_HH_MM_SS_mmm_zz2yyyymmdd_hhmmss_mmm_zz(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "float64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToFloat(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			default:
				panic("unhandled Type_ of field=" + row.Type)
			}
			ctrlMCheck = "_bsl[jj-1] == ''"
		} else if row.LastShown { // (LastShown != Last) implies that hidden columns follow
			io.WriteString(_fo, "if !self.Loadhidden_ {\n")
			switch row.Type {
			case "string":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = strings.TrimSpace(string(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "bool":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToBool(strings.TrimSpace(string(_bsl[ii:jj])),false); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "int64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),0); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyymmdd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),19000101); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyy_mm_dd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.YYYY_MM_DD2yyyymmdd(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "YYYY_MM_DD_HH_MM_SS_mmm_zz":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+", row."+row.Name+"_hhmmss"+endUnder+", row."+row.Name+"_mmm"+endUnder+", row."+row.Name+"_zz"+endUnder+" = genutil.YYYY_MM_DD_HH_MM_SS_mmm_zz2yyyymmdd_hhmmss_mmm_zz(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "float64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToFloat(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			default:
				panic("unhandled Type_ of field=" + row.Type)
			}
			io.WriteString(_fo, "} else {\n")
			switch row.Type {
			case "string":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = strings.TrimSpace(string(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "bool":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.ToBool(strings.TrimSpace(string(_bsl[ii:jj])),false); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "int64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),0); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyymmdd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),19000101); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyy_mm_dd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.YYYY_MM_DD2yyyymmdd(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "YYYY_MM_DD_HH_MM_SS_mmm_zz":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+", row."+row.Name+"_hhmmss"+endUnder+", row."+row.Name+"_mmm"+endUnder+", row."+row.Name+"_zz"+endUnder+" = genutil.YYYY_MM_DD_HH_MM_SS_mmm_zz2yyyymmdd_hhmmss_mmm_zz(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "float64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--; mm = 2}; row."+row.Name+endUnder+" = genutil.ToFloat(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			default:
				panic("unhandled Type_ of field=" + row.Type)
			}
			ctrlMCheck = "_bsl[jj-1] == ''"
		} else if row.Last { // (LastShown != Last) so this is the last hidden column
			switch row.Type {
			case "string":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = strings.TrimSpace(string(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "bool":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToBool(strings.TrimSpace(string(_bsl[ii:jj])),false); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "int64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),0); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyymmdd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToInt(strings.TrimSpace(string(_bsl[ii:jj])),19000101); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "yyyy_mm_dd":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.YYYY_MM_DD2yyyymmdd(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "YYYY_MM_DD_HH_MM_SS_mmm_zz":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+", row."+row.Name+"_hhmmss"+endUnder+", row."+row.Name+"_mmm"+endUnder+", row."+row.Name+"_zz"+endUnder+" = genutil.YYYY_MM_DD_HH_MM_SS_mmm_zz2yyyymmdd_hhmmss_mmm_zz(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			case "float64":
				io.WriteString(_fo, "   for ii = jj; jj < lenslice ; jj++ { if((_bsl[jj] == comma) || (jj+1 == lenslice)){ if "+ctrlMCheck+" {jj--        }; row."+row.Name+endUnder+" = genutil.ToFloat(bytes.TrimSpace(_bsl[ii:jj])); if(print) { fmt.Println(\""+row.Name+"=\", row."+row.Name+endUnder+"); }; jj +=mm; break; } }\n")
			default:
				panic("unhandled Type_ of field=" + row.Type)
			}
			io.WriteString(_fo, "}\n")
			ctrlMCheck = "_bsl[jj-1] == ''"
		}
	}
	io.WriteString(_fo, "   ok := _procRowFunc(row)\n")
	io.WriteString(_fo, "   if !ok { fmt.Println(\""+opt.Pkg+" bad row=\", string(_bsl)) }\n")
	io.WriteString(_fo, "   return row\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// AddRow adds a row into the in-memory representation of thie file format\n")
	io.WriteString(_fo, "func (self *"+capsName+") AddRow(_row *"+capsName+"Elem) (*"+capsName+"Elem, bool) {\n")
	io.WriteString(_fo, "   goodnum, ok := 0, true\n")
	if needDropRowInt64 {
		io.WriteString(_fo, "    var ki int64\n")
	}
	io.WriteString(_fo, "    var kk string\n")
	warnOnFirstIndex := true
	for _, im := range sortedIndexVals { // loop thru all the discovered indexes
		switch im.Type {
		case "int64":
			io.WriteString(_fo, "    ki = _row."+im.Rows[0]+"_;")
			io.WriteString(_fo, "   if true { self.Map"+im.Name+"2"+capsName+"[ki]  = append(self.Map"+im.Name+"2"+capsName+"[ki], _row) ; goodnum++ }\n")
		case "string":
			kk := ""                      // initialize the multipart key to the null string
			for ii, ip := range im.Rows { // loop thru all parts of this multipart index
				if ii > 0 {
					kk = kk + " + \"" + im.Sep + "\" + "
				} // if it is not the first part, add keysep
				kk = kk + " _row." + ip + endUnder // build up the multipart key by appending a part
			}
			// now output the statement to add the filerow to this index
			io.WriteString(_fo, "    kk = "+kk+"; ")
			io.WriteString(_fo, "   if((len(kk) > 0) || self.Nullkey_) { self.Map"+im.Name+"2"+capsName+"[kk]  = append(self.Map"+im.Name+"2"+capsName+"[kk], _row) ; goodnum++ }")
			if warnOnFirstIndex {
				warnOnFirstIndex = false
				io.WriteString(_fo, " else { fmt.Println(\"AddRow:"+capsName+": WARNING: Empty key will not get row added to outputting map\") }")
			}
			io.WriteString(_fo, "\n")
		}
		// io.WriteString(_fo, "   if(len(_row." + row.Name + endUnder + ") > 0) { self.Map" + row.Name + "2" + capsName + "[_row." + row.Name + endUnder + "]  = append(self.Map" + row.Name + "2" + capsName + "[_row." + row.Name + endUnder + "], _row) ; goodnum++ }\n")
	}

	io.WriteString(_fo, "   if(goodnum == 0) { ok = false; self.PrintRow(_row) } else { self.Numrows_++ } \n")
	io.WriteString(_fo, "   return _row, ok\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	// create functions for each index
	//		(1) that will find existing (or newly create an unadded) element using that index
	//		(2) that will test if there is an existing element using that index
	for _, im := range sortedIndexVals { // loop thru all the discovered indexes
		switch im.Type {
		case "int64":
			io.WriteString(_fo, "// FindOrNew"+im.Name+" returns slice consisting of all rows with matching key of specific named index\n")
			io.WriteString(_fo, "//    If no such rows exist, it creates an initialized slice of one row (but does not add that row)\n")
			io.WriteString(_fo, "func (self *"+capsName+") FindOrNew"+im.Name+"(_ki int64) ("+capsName+"ElemPtrSlice, bool) {\n")
			io.WriteString(_fo, "	rows, ok	:= self.Map"+im.Name+"2"+capsName+"[_ki]\n")
			io.WriteString(_fo, "	if ok { return rows, true }\n")
			io.WriteString(_fo, "	rows   = []"+capsName+"ElemPtr{new("+capsName+"Elem)}\n")
			io.WriteString(_fo, "	self.ClearRow(rows[0])\n")
			io.WriteString(_fo, "	return rows, false\n")
			io.WriteString(_fo, "}\n")
			io.WriteString(_fo, "\n")

			io.WriteString(_fo, "// HasMap"+im.Name+" returns bool testing if there exists atleast 1 row with matching key of specific named index\n")
			io.WriteString(_fo, "func (self *"+capsName+") HasMap"+im.Name+"(_ki int64) bool {\n")
			io.WriteString(_fo, "	rows, ok	:= self.Map"+im.Name+"2"+capsName+"[_ki]\n")
			io.WriteString(_fo, "	return ok && (len(rows) > 0) && (rows[0] != nil)\n")
			io.WriteString(_fo, "}\n")
			io.WriteString(_fo, "\n")

		case "string":
			io.WriteString(_fo, "// FindOrNew"+im.Name+" returns slice consisting of all rows with matching key of specific named index\n")
			io.WriteString(_fo, "//    If no such rows exist, it creates an initialized slice of one row (but does not add that row)\n")
			io.WriteString(_fo, "func (self *"+capsName+") FindOrNew"+im.Name+"(_kk string) ("+capsName+"ElemPtrSlice, bool) {\n")
			io.WriteString(_fo, "	rows, ok	:= self.Map"+im.Name+"2"+capsName+"[_kk]\n")
			io.WriteString(_fo, "	if ok { return rows, true }\n")
			io.WriteString(_fo, "	rows   = []"+capsName+"ElemPtr{new("+capsName+"Elem)}\n")
			io.WriteString(_fo, "	self.ClearRow(rows[0])\n")
			io.WriteString(_fo, "	return rows, false\n")
			io.WriteString(_fo, "}\n")
			io.WriteString(_fo, "\n")

			io.WriteString(_fo, "// HasMap"+im.Name+" returns bool testing if there exists atleast 1 row with matching key of specific named index\n")
			io.WriteString(_fo, "func (self *"+capsName+") HasMap"+im.Name+"(_kk string) bool {\n")
			io.WriteString(_fo, "	rows, ok	:= self.Map"+im.Name+"2"+capsName+"[_kk]\n")
			io.WriteString(_fo, "	return ok && (len(rows) > 0) && (rows[0] != nil)\n")
			io.WriteString(_fo, "}\n")
			io.WriteString(_fo, "\n")
		}
	}

	// create function to return sorted values and sorted keys for each index
	for _, im := range sortedIndexVals { // loop thru all the discovered indexes
		switch im.Type {
		case "int64":
			io.WriteString(_fo, "// SortedKeys_Map"+im.Name+"2"+capsName+" returns slice consisting of keys in the specific named index\n")
			io.WriteString(_fo, "func (self *"+capsName+") SortedKeys_Map"+im.Name+"2"+capsName+"() []int64 {\n")
			io.WriteString(_fo, "	keys := make([]int, len(self.Map"+im.Name+"2"+capsName+"))\n")
			io.WriteString(_fo, "	ii	:= 0\n")
			io.WriteString(_fo, "	for kk := range self.Map"+im.Name+"2"+capsName+"{\n")
			io.WriteString(_fo, "		keys[ii] = int(kk)\n")
			io.WriteString(_fo, "		ii++\n")
			io.WriteString(_fo, "        }\n")
			io.WriteString(_fo, "		sort.Ints(keys)\n")
			io.WriteString(_fo, "		vals := make([]int64, len(keys))\n")
			io.WriteString(_fo, "		ii = 0\n")
			io.WriteString(_fo, "		for ii, kk := range keys {\n")
			io.WriteString(_fo, "			vals[ii] = int64(kk)\n")
			io.WriteString(_fo, "               }\n")
			io.WriteString(_fo, "	return vals}\n")
			io.WriteString(_fo, "\n")

			io.WriteString(_fo, "// Sorted_Map"+im.Name+"2"+capsName+" returns slice (whose each elem is a slice of row with specific key value) for sorted keys of a specific index\n")
			io.WriteString(_fo, "func (self *"+capsName+") Sorted_Map"+im.Name+"2"+capsName+"() []"+capsName+"ElemPtrSlice {\n")
			io.WriteString(_fo, "	keys := make([]int, len(self.Map"+im.Name+"2"+capsName+"))\n")
			io.WriteString(_fo, "	ii	:= 0\n")
			io.WriteString(_fo, "	for kk := range self.Map"+im.Name+"2"+capsName+"{\n")
			io.WriteString(_fo, "		keys[ii] = int(kk)\n")
			io.WriteString(_fo, "		ii++\n")
			io.WriteString(_fo, "       }\n")
			io.WriteString(_fo, "		sort.Ints(keys)\n")
			io.WriteString(_fo, "		vals := make([]"+capsName+"ElemPtrSlice, len(keys))\n")
			io.WriteString(_fo, "		ii = 0\n")
			io.WriteString(_fo, "		for ii, kk := range keys {\n")
			io.WriteString(_fo, "			vals[ii] = self.Map"+im.Name+"2"+capsName+"[int64(kk)]\n")
			io.WriteString(_fo, "       }\n")
			io.WriteString(_fo, "	return vals}\n")
			io.WriteString(_fo, "\n")

		case "string":
			io.WriteString(_fo, "// SortedKeys_Map"+im.Name+"2"+capsName+" returns slice consisting of keys in the specific named index\n")
			io.WriteString(_fo, "func (self *"+capsName+") SortedKeys_Map"+im.Name+"2"+capsName+"() []string {\n")
			io.WriteString(_fo, "	keys := make([]string, len(self.Map"+im.Name+"2"+capsName+"))\n")
			io.WriteString(_fo, "	ii	:= 0\n")
			io.WriteString(_fo, "	for kk := range self.Map"+im.Name+"2"+capsName+"{\n")
			io.WriteString(_fo, "		keys[ii] = kk\n")
			io.WriteString(_fo, "		ii++\n")
			io.WriteString(_fo, "       }\n")
			io.WriteString(_fo, "		sort.Strings(keys)\n")
			io.WriteString(_fo, "	return keys}\n")
			io.WriteString(_fo, "\n")

			io.WriteString(_fo, "// Sorted_Map"+im.Name+"2"+capsName+" returns slice (whose each elem is a slice of row with specific key value) for sorted keys of a specific index\n")
			io.WriteString(_fo, "func (self *"+capsName+") Sorted_Map"+im.Name+"2"+capsName+"() []"+capsName+"ElemPtrSlice {\n")
			io.WriteString(_fo, "	keys := make([]string, len(self.Map"+im.Name+"2"+capsName+"))\n")
			io.WriteString(_fo, "	ii	:= 0\n")
			io.WriteString(_fo, "	for kk := range self.Map"+im.Name+"2"+capsName+"{\n")
			io.WriteString(_fo, "		keys[ii] = kk\n")
			io.WriteString(_fo, "		ii++\n")
			io.WriteString(_fo, "}\n")
			io.WriteString(_fo, "		sort.Strings(keys)\n")
			io.WriteString(_fo, "		vals := make([]"+capsName+"ElemPtrSlice, len(keys))\n")
			io.WriteString(_fo, "		ii = 0\n")
			io.WriteString(_fo, "		for ii, kk := range keys {\n")
			io.WriteString(_fo, "			vals[ii] = self.Map"+im.Name+"2"+capsName+"[kk]\n")
			io.WriteString(_fo, "}\n")
			io.WriteString(_fo, "	return vals}\n")
			io.WriteString(_fo, "\n")

		}
	}

	// ========================================================
	io.WriteString(_fo, "// Load loads all the rows from a file to the in-memory representation\n")
	io.WriteString(_fo, "func (self *"+capsName+") Load (_fname string) *"+capsName+"{\n")
	io.WriteString(_fo, "    rr := genutil.OpenAny(_fname)\n")
	io.WriteString(_fo, "    if rr == nil {\n")
	io.WriteString(_fo, "	panic(\""+capsName+": Load : bad file=\" + _fname)\n")
	io.WriteString(_fo, "    }\n")
	io.WriteString(_fo, "    numread, numbad := 0, 0\n")
	io.WriteString(_fo, "    for first := true;;first = false {\n")
	io.WriteString(_fo, "        bsl, err	:= rr.ReadSlice('"+"\\"+"n')\n")
	io.WriteString(_fo, "        if err != nil && err != io.EOF { log.Panicf(\""+capsName+".Load: Error (%s) in ReadSlice for fname(%s)\", err.Error(), _fname) }\n")
	io.WriteString(_fo, "	if(err == io.EOF) { break }\n")
	io.WriteString(_fo, "	if(len(bsl) < 1) { numbad++; continue }\n")

	ii := 0
	switch opt.HeaderStyle {
	case "external":
		io.WriteString(_fo, "	if(")
		for ii = 0; ii < len(arr[0].Headerstring); ii++ {
			iis := strconv.FormatInt(int64(ii), 10)
			iic := fmt.Sprintf("%c", arr[0].Headerstring[ii])
			io.WriteString(_fo, "(bsl["+iis+"] == '"+iic+"') && ")
		}
		io.WriteString(_fo, "(bsl["+strconv.FormatInt(int64(len(arr[0].Headerstring)), 10)+"] == ',')) { if(!first) { numbad++ }; continue }\n")
	default:
		io.WriteString(_fo, "	if(")
		for ii = 0; ii < len(arr[0].Name); ii++ {
			iis := strconv.FormatInt(int64(ii), 10)
			iic := fmt.Sprintf("%c", arr[0].Name[ii])
			io.WriteString(_fo, "(bsl["+iis+"] == '"+iic+"') && ")
		}
		io.WriteString(_fo, "(bsl["+strconv.FormatInt(int64(len(arr[0].Name)), 10)+"] == ',')) { if(!first) { numbad++ }; continue }\n")
	}

	io.WriteString(_fo, "	if(!first) {\n")
	io.WriteString(_fo, "		self.loadElem(bsl)\n")
	io.WriteString(_fo, "		numread++\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "    }\n")
	io.WriteString(_fo, "  if !self.Silent_ {  fmt.Println(\""+opt.Pkg+" numread=\", numread, \" numbad=\", numbad,\n")
	done1 := false
	for _, row := range sortedIndexVals {
		if done1 {
			io.WriteString(_fo, "		     ,")
		} else {
			io.WriteString(_fo, "		     ")
		}
		io.WriteString(_fo, "\" num"+row.Name+"=\", len(self.Map"+row.Name+"2"+capsName+")")
		done1 = true
	}
	io.WriteString(_fo, "	, genutil.FileInfo(_fname, \" \", false))\n")
	for _, row := range sortedIndexVals {
		io.WriteString(_fo, "    // fmt.Println(\""+row.Name+"s : ======\"); for _, rows := range self.Map"+row.Name+"2"+capsName+" { self.PrintRows(rows) }\n")
	}
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, " if len(self.LoadedFilename_) == 0 {self.LoadedFilename_=_fname} else {self.LoadedFilename_ += \";\" + _fname}\n")
	io.WriteString(_fo, "   self.Numread_		= numread\n")
	io.WriteString(_fo, "   return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// LoadIfExists loads all the rows from a file to the in-memory representation, but does not fail if the filename does not exist\n")
	io.WriteString(_fo, "func (self *"+capsName+") LoadIfExists (_fname string) *"+capsName+"{\n")
	io.WriteString(_fo, "	if genutil.AnyPathOK(_fname) { return self.Load(_fname) }\n")
	io.WriteString(_fo, "		return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// LoadBuf loads all the rows from a buffer into the in-memory representation, but does not fail if the filename does not exist\n")
	io.WriteString(_fo, "func (self *"+capsName+") LoadBuf (_fname string, _buffer []byte) *"+capsName+"{\n")
	io.WriteString(_fo, "    numread, buflen := 0, len(_buffer)\n")
	io.WriteString(_fo, "\n")
	io.WriteString(_fo, "	// skip header\n")
	io.WriteString(_fo, "    rowBegin := 0\n")
	io.WriteString(_fo, "    rowEnd := genutil.IndexNl (_buffer, buflen, 0)\n")
	io.WriteString(_fo, "    if rowEnd > buflen { log.Panicf(\""+capsName+".LoadBuf: Error: Failed to parse newline in buffer for fname(%s)\", _fname) }\n")
	io.WriteString(_fo, "    rowBegin = rowEnd \n")
	io.WriteString(_fo, "\n")
	io.WriteString(_fo, "	// parse the remaining lines\n")
	io.WriteString(_fo, "	for ; rowBegin < buflen; rowBegin = rowEnd {\n")
	io.WriteString(_fo, "   	rowEnd = genutil.IndexNl (_buffer, buflen, rowBegin)\n")
	io.WriteString(_fo, "		bsl := _buffer[rowBegin:rowEnd]\n")
	io.WriteString(_fo, "		self.loadElem(bsl)\n")
	io.WriteString(_fo, "		numread++\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "   if !self.Silent_ {  fmt.Println(\""+opt.Pkg+" numread=\", numread) }\n")
	io.WriteString(_fo, "   self.LoadedFilename_=_fname\n")
	io.WriteString(_fo, "   return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// Proc processes all the rows from a file but unlike Load it does not put them into the in-memory representation\n")
	io.WriteString(_fo, "func (self *"+capsName+") Proc (_fname string, _procRowFunc ProcRowFunc"+capsName+") *"+capsName+"{\n")
	io.WriteString(_fo, "    rr := genutil.OpenAny(_fname)\n")
	io.WriteString(_fo, "    if rr == nil {\n")
	io.WriteString(_fo, "	panic(\""+capsName+": Proc : bad file=\" + _fname)\n")
	io.WriteString(_fo, "    }\n")
	io.WriteString(_fo, "    numread, numbad := 0, 0\n")
	io.WriteString(_fo, "    for first := true;;first = false {\n")
	io.WriteString(_fo, "        bsl, err	:= rr.ReadSlice('"+"\\"+"n')\n")
	io.WriteString(_fo, "        if err != nil && err != io.EOF { log.Panicf(\""+capsName+".Proc: Error (%s) in ReadSlice for fname(%s)\", err.Error(), _fname) }\n")
	io.WriteString(_fo, "	if(err == io.EOF) { break }\n")
	io.WriteString(_fo, "	if(len(bsl) < 1) { numbad++; continue }\n")

	io.WriteString(_fo, "	if(")
	qq := 0
	for qq = 0; qq < len(arr[0].Name); qq++ {
		qqs := strconv.FormatInt(int64(qq), 10)
		qqc := fmt.Sprintf("%c", arr[0].Name[qq])
		io.WriteString(_fo, "(bsl["+qqs+"] == '"+qqc+"') && ")
	}
	io.WriteString(_fo, "(bsl["+strconv.FormatInt(int64(len(arr[0].Name)), 10)+"] == ',')) { if(!first) { numbad++ }; continue }\n")

	io.WriteString(_fo, "	if(!first) {\n")
	io.WriteString(_fo, "		self.procElem(bsl, _procRowFunc)\n")
	io.WriteString(_fo, "		numread++\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "    }\n")
	io.WriteString(_fo, "  if !self.Silent_ {  fmt.Println(\""+opt.Pkg+" numread=\", numread, \" numbad=\", numbad,\n")
	done2 := false
	for _, row := range sortedIndexVals {
		if done2 {
			io.WriteString(_fo, "		     ,")
		} else {
			io.WriteString(_fo, "		     ")
		}
		io.WriteString(_fo, "\" num"+row.Name+"=\", len(self.Map"+row.Name+"2"+capsName+")")
		done2 = true
	}
	io.WriteString(_fo, "	, \"fname=\", _fname	)\n")
	for _, row := range sortedIndexVals {
		io.WriteString(_fo, "    // fmt.Println(\""+row.Name+"s : ======\"); for _, rows := range self.Map"+row.Name+"2"+capsName+" { self.PrintRows(rows) }\n")
	}
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, " if len(self.LoadedFilename_) == 0 {self.LoadedFilename_=_fname} else {self.LoadedFilename_ += \";\" + _fname}\n")
	io.WriteString(_fo, "   self.Numread_		= numread\n")
	io.WriteString(_fo, "   return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// ProcFuncSample illustrates how to use Proc\n")
	io.WriteString(_fo, " func ProcFuncSample(_row *"+capsName+"Elem) bool {\n")
	io.WriteString(_fo, " 	fmt.Println(\"foo\")\n")
	io.WriteString(_fo, " 	return true\n")
	io.WriteString(_fo, " }\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// SortwriteFile writes the in-memory representation to file, in sorted order \n")
	io.WriteString(_fo, "func (self *"+capsName+") SortwriteFile(_ofile string) *"+capsName+" {\n")
	io.WriteString(_fo, "	ww	:= genutil.OpenGzFile(_ofile)\n")
	io.WriteString(_fo, "	defer ww.Close()\n")
	io.WriteString(_fo, "	count := 0\n")
	io.WriteString(_fo, "	hdr := \"")

	for _, row := range arr {
		if row.Hidden {
			continue
		}
		if !row.FirstShown {
			io.WriteString(_fo, ",")
		}
		switch opt.HeaderStyle {
		case "external":
			io.WriteString(_fo, row.Headerstring)
		default:
			io.WriteString(_fo, row.Name)
		}
		if row.LastShown {
			io.WriteString(_fo, "\"\n")
		}
	}

	io.WriteString(_fo, "	fmt.Fprintf(ww, \"%s\\n\", hdr)\n")
	io.WriteString(_fo, "	for _, rows := range self.Sorted_Map"+favIM.Name+"2"+capsName+"() {\n")
	io.WriteString(_fo, "		count += self.WriteRows(ww, rows)\n")
	io.WriteString(_fo, "	}\n")
	io.WriteString(_fo, "	if false { fmt.Println(\""+capsName+".SortwriteFile: ofile=\", _ofile, \"count=\", count) }\n")
	io.WriteString(_fo, "	return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// WriteFile writes the in-memory representation to file\n")
	io.WriteString(_fo, "func (self *"+capsName+") WriteFile(_ofile string) *"+capsName+" {\n")
	io.WriteString(_fo, "	ww	:= genutil.OpenGzFile(_ofile)\n")
	io.WriteString(_fo, "	defer ww.Close()\n")
	io.WriteString(_fo, "	count := 0\n")
	io.WriteString(_fo, "	hdr := \"")

	for _, row := range arr {
		if row.Hidden {
			continue
		}
		if !row.FirstShown {
			io.WriteString(_fo, ",")
		}
		switch opt.HeaderStyle {
		case "external":
			io.WriteString(_fo, row.Headerstring)
		default:
			io.WriteString(_fo, row.Name)
		}
		if row.LastShown {
			io.WriteString(_fo, "\"\n")
		}
	}

	io.WriteString(_fo, "	fmt.Fprintf(ww, \"%s\\n\", hdr)\n")
	io.WriteString(_fo, "	for _, rows := range self.Map"+sortedIndexVals[0].Name+"2"+capsName+" {\n")
	io.WriteString(_fo, "		count += self.WriteRows(ww, rows)\n")
	io.WriteString(_fo, "	}\n")
	io.WriteString(_fo, "	if false { fmt.Println(\""+capsName+".WriteFile: ofile=\", _ofile, \"count=\", count) }\n")
	io.WriteString(_fo, "	return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// WriteFileHidden writes the in-memory representation, including hidden columns, to file\n")
	io.WriteString(_fo, "func (self *"+capsName+") WriteFileHidden(_ofile string) *"+capsName+" {\n")
	io.WriteString(_fo, "	ww	:= genutil.OpenGzFile(_ofile)\n")
	io.WriteString(_fo, "	defer ww.Close()\n")
	io.WriteString(_fo, "	count := 0\n")
	io.WriteString(_fo, "	hdr := \"")

	for _, row := range arr {
		if row.Header || row.Footer {
			continue
		}
		// if row.Hidden { continue }
		if !row.Footer {
			if !row.FirstShown {
				io.WriteString(_fo, ",")
			}
			switch opt.HeaderStyle {
			case "external":
				io.WriteString(_fo, row.Headerstring)
			default:
				io.WriteString(_fo, row.Name)
			}
		}
		if row.Last {
			io.WriteString(_fo, "\"\n")
		}
	}

	io.WriteString(_fo, "	fmt.Fprintf(ww, \"%s\\n\", hdr)\n")
	io.WriteString(_fo, "	for _, rows := range self.Map"+sortedIndexVals[0].Name+"2"+capsName+" {\n")
	io.WriteString(_fo, "		count += self.WriteRowsHidden(ww, rows)\n")
	io.WriteString(_fo, "	}\n")
	io.WriteString(_fo, "	if false { fmt.Println(\""+capsName+".WriteFileHidden: ofile=\", _ofile, \"count=\", count) }\n")
	io.WriteString(_fo, "	return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// SortwriteFileHidden writes the in-memory representation, including hidden columns, to file, in sorted order\n")
	io.WriteString(_fo, "func (self *"+capsName+") SortwriteFileHidden(_ofile string) *"+capsName+" {\n")
	io.WriteString(_fo, "	ww	:= genutil.OpenGzFile(_ofile)\n")
	io.WriteString(_fo, "	defer ww.Close()\n")
	io.WriteString(_fo, "	count := 0\n")
	io.WriteString(_fo, "	hdr := \"")

	for _, row := range arr {
		if row.Header || row.Footer {
			continue
		}
		if !row.Footer {
			// if row.Hidden { continue }
			if !row.FirstShown {
				io.WriteString(_fo, ",")
			}
			switch opt.HeaderStyle {
			case "external":
				io.WriteString(_fo, row.Headerstring)
			default:
				io.WriteString(_fo, row.Name)
			}
		}
		if row.Last {
			io.WriteString(_fo, "\"\n")
		}
	}

	io.WriteString(_fo, "	fmt.Fprintf(ww, \"%s\\n\", hdr)\n")
	io.WriteString(_fo, "	for _, rows := range self.Sorted_Map"+favIM.Name+"2"+capsName+"() {\n")
	io.WriteString(_fo, "		count += self.WriteRowsHidden(ww, rows)\n")
	io.WriteString(_fo, "	}\n")
	io.WriteString(_fo, "	if false { fmt.Println(\""+capsName+".SortwriteFileHidden: ofile=\", _ofile, \"count=\", count) }\n")
	io.WriteString(_fo, "	return self\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// WriteRows writes the rows in the passed slice\n")
	io.WriteString(_fo, "func (self *"+capsName+") WriteRows(_ww io.Writer, _rows "+capsName+"ElemPtrSlice) int {\n")
	io.WriteString(_fo, "     count := 0\n")
	io.WriteString(_fo, "     for _, row := range _rows {\n")
	io.WriteString(_fo, "     	 self.WriteRow(_ww, row)\n")
	io.WriteString(_fo, "     count = count + 1\n")
	io.WriteString(_fo, "     }\n")
	io.WriteString(_fo, "     return count\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// WriteRowsHidden writes the rows, including hidden columns, in the passed slice\n")
	io.WriteString(_fo, "func (self *"+capsName+") WriteRowsHidden(_ww io.Writer, _rows "+capsName+"ElemPtrSlice) int {\n")
	io.WriteString(_fo, "     count := 0\n")
	io.WriteString(_fo, "     for _, row := range _rows {\n")
	io.WriteString(_fo, "     	 self.WriteRowHidden(_ww, row)\n")
	io.WriteString(_fo, "     count = count + 1\n")
	io.WriteString(_fo, "     }\n")
	io.WriteString(_fo, "     return count\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// WriteRow writes the specified row\n")
	io.WriteString(_fo, "func (self *"+capsName+") WriteRow(_ww io.Writer, _row "+capsName+"ElemPtr) {\n")
	for _, row := range arr {
		if row.Hidden {
			continue
		}
		nlval := ""
		if !row.FirstShown {
			nlval = ","
		}
		switch row.Type {
		case "string":
			io.WriteString(_fo, "	fmt.Fprintf(_ww, \""+nlval+"%s\", _row."+row.Name+endUnder+")\n")
		case "bool":
			io.WriteString(_fo, "	fmt.Fprintf(_ww, \""+nlval+"%s\", strconv.FormatBool(_row."+row.Name+endUnder+"))\n")
		case "int64":
			io.WriteString(_fo, "	fmt.Fprintf(_ww, \""+nlval+"%s\", strconv.FormatInt(_row."+row.Name+endUnder+", 10))\n")
		case "yyyymmdd":
			io.WriteString(_fo, "	fmt.Fprintf(_ww, \""+nlval+"%s\", strconv.FormatInt(_row."+row.Name+endUnder+", 10))\n")
		case "yyyy_mm_dd":
			io.WriteString(_fo, "	fmt.Fprintf(_ww, \""+nlval+"%s\", strconv.FormatInt(_row."+row.Name+endUnder+", 10))\n")
		case "YYYY_MM_DD_HH_MM_SS_mmm_zz":
			io.WriteString(_fo, "	fmt.Fprintf(_ww, \""+nlval+"%s\", strconv.FormatInt(_row."+row.Name+endUnder+", 10))\n")
		case "float64":
			io.WriteString(_fo, "	fmt.Fprintf(_ww, \""+nlval+"%s\", strconv.FormatFloat(_row."+row.Name+endUnder+", 'f', 6, 64))\n")
		default:
			panic("unhandled Type_ of field=" + row.Type)
		}
	}
	io.WriteString(_fo, "	fmt.Fprintf(_ww, \"\\n\")")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// WriteRowHidden writes the specified row, including hidden columns\n")
	io.WriteString(_fo, "func (self *"+capsName+") WriteRowHidden(_ww io.Writer, _row "+capsName+"ElemPtr) {\n")
	for _, row := range arr {
		if row.Header || row.Footer {
			continue
		}
		nlval := ""
		if !row.FirstShown {
			nlval = ","
		}
		switch row.Type {
		case "string":
			io.WriteString(_fo, "	fmt.Fprintf(_ww, \""+nlval+"%s\", _row."+row.Name+endUnder+")\n")
		case "bool":
			io.WriteString(_fo, "	fmt.Fprintf(_ww, \""+nlval+"%s\", strconv.FormatBool(_row."+row.Name+endUnder+"))\n")
		case "int64":
			io.WriteString(_fo, "	fmt.Fprintf(_ww, \""+nlval+"%s\", strconv.FormatInt(_row."+row.Name+endUnder+", 10))\n")
		case "yyyymmdd":
			io.WriteString(_fo, "	fmt.Fprintf(_ww, \""+nlval+"%s\", strconv.FormatInt(_row."+row.Name+endUnder+", 10))\n")
		case "yyyy_mm_dd":
			io.WriteString(_fo, "	fmt.Fprintf(_ww, \""+nlval+"%s\", strconv.FormatInt(_row."+row.Name+endUnder+", 10))\n")
		case "YYYY_MM_DD_HH_MM_SS_mmm_zz":
			io.WriteString(_fo, "	fmt.Fprintf(_ww, \""+nlval+"%s\", strconv.FormatInt(_row."+row.Name+endUnder+", 10))\n")
		case "float64":
			io.WriteString(_fo, "	fmt.Fprintf(_ww, \""+nlval+"%s\", strconv.FormatFloat(_row."+row.Name+endUnder+", 'f', 6, 64))\n")
		default:
			panic("unhandled Type_ of field=" + row.Type)
		}
	}
	io.WriteString(_fo, "	fmt.Fprintf(_ww, \"\\n\")")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// ClearRow clears the specified row\n")
	io.WriteString(_fo, "func (self *"+capsName+") ClearRow(_row "+capsName+"ElemPtr) {\n")
	for _, row := range arr {
		if row.Header || row.Footer {
			continue
		}
		switch row.Type {
		case "string":
			io.WriteString(_fo, "	_row."+row.Name+endUnder+"	= \"\"\n")
		case "bool":
			io.WriteString(_fo, "	_row."+row.Name+endUnder+"	= false\n")
		case "int64":
			io.WriteString(_fo, "	_row."+row.Name+endUnder+"	= 0\n")
		case "yyyymmdd":
			io.WriteString(_fo, "	_row."+row.Name+endUnder+"	= 19000101\n")
		case "yyyy_mm_dd":
			io.WriteString(_fo, "	_row."+row.Name+endUnder+"	= 19000101\n")
		case "YYYY_MM_DD_HH_MM_SS_mmm_zz":
			io.WriteString(_fo, "	_row."+row.Name+endUnder+"	= 19000101\n")
		case "float64":
			io.WriteString(_fo, "	_row."+row.Name+endUnder+"	= 0.0\n")
		default:
			panic("unhandled Type_ of field=" + row.Type)
		}
	}
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	// ========================================================
	io.WriteString(_fo, "// CopyRow copies the specified row and returns the copy\n")
	io.WriteString(_fo, "func CopyRow(_from, _to "+capsName+"ElemPtr) "+capsName+"ElemPtr {\n")
	for _, row := range arr {
		if row.Header || row.Footer {
			continue
		}
		io.WriteString(_fo, "	_to."+row.Name+endUnder+"	= _from."+row.Name+endUnder+"\n")
	}
	io.WriteString(_fo, "  return _to\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	// ========================================================
	io.WriteString(_fo, "// LoadMustIfBiz will load from the file, but will panic if unable to load when isBiz is true\n")
	io.WriteString(_fo, "func (self *"+capsName+") LoadMustIfBiz(_fname string, _isBiz bool) *"+capsName+" {\n")
	io.WriteString(_fo, "    if _isBiz {	return self.Load(_fname) }\n")
	io.WriteString(_fo, "    return self.LoadIfExists(_fname)	// Loading is not mandatory on nonbiz day\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")
	// ========================================================
}

func main() {
	if parseArgs() {
		fmt.Println("gencsv ============================================================================================= starting")

		fo, err := os.Create(opt.Ofile)
		if err != nil {
			panic(err)
		}
		defer fo.Close()
		ft, err := os.Create(opt.TestMain)
		if err != nil {
			panic(err)
		}
		defer ft.Close()
		fd, err := os.Create(opt.TestBash)
		if err != nil {
			panic(err)
		}
		defer fd.Close()

		loadSpec(opt.Cfg)

		writePre(fo)
		writeStruct(fo)
		writeStructMore(fo)
		writeTest(ft)
		writeDoit(fd)
		genutil.BashExecOrDie(true, "chmod 775 "+opt.TestBash, ".")
		fmt.Println("gencsv ============================================================================================= done")
	} else {
		// Assume being called in gencfg mode - creates spec file from the single csv parameter
		fmt.Println("name,headerstring,type,hasindex,finaltype")
		parts := strings.Split(os.Args[1], ",")
		for _, row1 := range parts {
			row := row1
			row = strings.Replace(row, ".", "_", -1)
			row = strings.Replace(row, "/", "_", -1)
			row = strings.Replace(row, "-", "_", -1)
			row = strings.Replace(row, "&", "_", -1)
			row = strings.Replace(row, "\"", "", -1)
			row = strings.Replace(row, "[", "", -1)
			row = strings.Replace(row, "]", "", -1)
			row = strings.Replace(row, "(", "", -1)
			row = strings.Replace(row, ")", "", -1)
			row = strings.Replace(row, "%", "Pct", -1)
			row = strings.Replace(row, "$", "USD", -1)
			row = strings.Replace(row, "#", "Num", -1)
			row = strings.Replace(row, " ", "", -1)
			row = strings.ToUpper(row[0:1]) + strings.ToLower(row[1:])
			str := row + "," + row1 + ",,,"
			fmt.Println(str)
		}
	}
}
