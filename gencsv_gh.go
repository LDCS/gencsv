package main

import (
	"github.com/LDCS/genutil"
	"io"
)

func writeStructMore(_fo io.Writer) {
	// ========================================================
	io.WriteString(_fo, "// WriteFileStart returns bufio.Writer to the specified file, after writing the header row\n")
	io.WriteString(_fo, "func (self *"+capsName+") WriteFileStart(_ofile string)  genutil.GzFile {\n")
	io.WriteString(_fo, "	ww	:= genutil.OpenGzFile(_ofile)\n")
	io.WriteString(_fo, "	fmt.Fprintf(ww, \"")
	for _, row := range arr {
		if row.Hidden {
			continue
		}
		switch opt.HeaderStyle {
		case "external":
			io.WriteString(_fo, row.Headerstring)
		default:
			io.WriteString(_fo, row.Name)
		}
		if !row.LastShown {
			io.WriteString(_fo, ",")
		}
	}
	io.WriteString(_fo, "\\n\")\n")
	io.WriteString(_fo, "	return ww\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// WriteFileEnd flushes and closes the bufio.Writer\n")
	io.WriteString(_fo, "func (self *"+capsName+") WriteFileEnd(_ww genutil.GzFile) {\n")
	io.WriteString(_fo, "     _ww.Close()\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	io.WriteString(_fo, "// PointerMap is useful for dealing with collections of *"+capsName+"\n")
	io.WriteString(_fo, "func PointerMap() *map[string]*"+capsName+" {\n")
	io.WriteString(_fo, "	ptrmap	:= map[string]*"+capsName+"{}\n")
	io.WriteString(_fo, "	return &ptrmap\n")
	io.WriteString(_fo, "}\n")
	io.WriteString(_fo, "\n")

	// ========================================================
	// create functions for each index
	//		(1) that will find existing (or newly create an unadded) element using that index
	//		(2) that will test if there is an existing element using that index
	for _, yrow := range yarr { // loop thru all the instance variables
		switch {
		case len(yrow.Hasindex) < 1:
			continue
		case yrow.Hasindex != "sort":
			continue
		}

		unixNano := genutil.StrTernary(yrow.Type == "time.Time", ".UnixNano()", "")

		io.WriteString(_fo, "type valueSliceBy"+yrow.Name+" []*"+capsName+"\n")
		io.WriteString(_fo, "func (pp valueSliceBy"+yrow.Name+") Len() int { return len(pp) }\n")
		io.WriteString(_fo, "func (pp valueSliceBy"+yrow.Name+") Less(ii, jj int) bool { return pp[ii]."+yrow.Name+"_"+unixNano+" < pp[jj]."+yrow.Name+"_"+unixNano+" }\n")
		io.WriteString(_fo, "func (pp valueSliceBy"+yrow.Name+") Swap(ii, jj int) { pp[ii], pp[jj] = pp[jj], pp[ii] }\n")
		io.WriteString(_fo, "func ValuesOfPointerMapSortedBy"+yrow.Name+"(_map *map[string]*"+capsName+") (_rows []*"+capsName+") {\n")
		io.WriteString(_fo, "	arr	:= make(valueSliceBy"+yrow.Name+", 0, len(*_map))\n")
		io.WriteString(_fo, "	for _, aa := range *_map {\n")
		io.WriteString(_fo, "		arr	= append(arr, aa)\n")
		io.WriteString(_fo, "	}\n")
		io.WriteString(_fo, "	sort.Sort(arr)\n")
		io.WriteString(_fo, "	return arr\n")
		io.WriteString(_fo, "}\n")
		io.WriteString(_fo, "type keyBy"+yrow.Name+" struct {\n")
		io.WriteString(_fo, "	kk string\n")
		io.WriteString(_fo, "	vv *"+capsName+" \n")
		io.WriteString(_fo, "}\n")
		io.WriteString(_fo, "type keySliceBy"+yrow.Name+" []keyBy"+yrow.Name+"\n")
		io.WriteString(_fo, "func (pp keySliceBy"+yrow.Name+") Len() int { return len(pp) }\n")
		io.WriteString(_fo, "func (pp keySliceBy"+yrow.Name+") Less(ii, jj int) bool { return pp[ii].vv."+yrow.Name+"_"+unixNano+" < pp[jj].vv."+yrow.Name+"_"+unixNano+" }\n")
		io.WriteString(_fo, "func (pp keySliceBy"+yrow.Name+") Swap(ii, jj int) { pp[ii], pp[jj] = pp[jj], pp[ii] }\n")
		io.WriteString(_fo, "func KeysOfPointerMapSortedBy"+yrow.Name+"(_map *map[string]*"+capsName+") []string {\n")
		io.WriteString(_fo, "	arr			:= make(keySliceBy"+yrow.Name+", 0, len(*_map))\n")
		io.WriteString(_fo, "	for kk, aa := range *_map {\n")
		io.WriteString(_fo, "		arr		= append(arr, keyBy"+yrow.Name+"{kk, aa})\n")
		io.WriteString(_fo, "	}\n")
		io.WriteString(_fo, "	sort.Sort(arr)\n")
		io.WriteString(_fo, "	strarr	:= make([]string, len(*_map))\n")
		io.WriteString(_fo, "	for ii, _ := range arr {\n")
		io.WriteString(_fo, "		strarr[ii] = arr[ii].kk\n")
		io.WriteString(_fo, "	}\n")
		io.WriteString(_fo, "	return strarr\n")
		io.WriteString(_fo, "}\n")
		io.WriteString(_fo, "\n")
	}
}
