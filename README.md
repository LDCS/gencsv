# gencsv
gencsv generates a package specific to the hcsv file format described in its input.


An hcsv file is a csv file that
  1. uses comma as separator
  2. has a header row containing unique non-blank column names
  3. has the same number of commas in each row
  4. has cell values, that are valid when trimmed of whitespace

The generated package defines a struct corresponding to one row of the target format.
It has functions to read/write files with the target format to/from in-memory arrays.
The in-memory array is indexed using possibly multiple, possibly multicolumn indexes.
One may also define additional "hidden" columns which are useful during processing.
One may also define "instance" variables which are at the file level.

Once familiar with the procedure, new formats can be added to workflow within minutes.

Gencsv could be useful to an entity involved in dynamic ETL data processing involving csv files of varied formats.
These typically arise from engagement with several counterparties.
As an example, consider financial fund with largely automated processes which must engage with new counterparties.
In this example, counterparties include multiple prime brokers, execution brokers, funds administrator, data vendors, reporting entities etc.

Gencsv does not use reflection anywhere.
Instead package generation runs off a spec file that lists properties of each column of the format.
For each column named (say FOO) in the target format, the spec file should contain a "spec row" for FOO.
This row specifies properties of column FOO (its member name, display name, type, indexes, visibility).
Each hidden column (say BAR) would require "spec row" for BAR.

Hidden columns are initialized to nil when target format files are read, and not written when target files are written.
(Unless the corresponding *Hidden funcs are called).
The column names that are written out can be specified with the --HeaderStyle commandline parameter.
1. internal - the golang-legal names of the corresponding in-memory struct members
2. external - taken from the "headerstring" column of the spec file

Gencsv can be called in 2 modes
  1. GENCFG: to generate the spec file
  2. GENCSV: to generate the package file (from the spec file)

A spec file created in GENCFG mode must be hand-edited before it can be used in GENCSV mode.
At minimum, you must set one favourite index
Simply putting "*index" in the "hasindex" column of spec row FOO will generate a single-column index on target format column FOO.

But if there is even one multi-column index, you will have to specify ALL indexes in the longhand form.
In longhand form the hasindex column describes how column FOO participates in each index, BAZ, where it participates.
The string "index(BAZ=N=/)" in the hasindex column of spec row FOO specifies that column FOO is part of the multi-column index named BAZ.
  Further, it is in (0-indexed) position N and the key is formed by joining component key values with separator "/".
  If preceded with "*" BAZ is noted to be the favourite index, i.e, the order to be used when writing out the file in sorted order.
For participation in a number of indexes, just concatenate index descriptions.

The package file which is created should not be hand edited.
Often, you will decide you want change the number or components of the indexes.
To do so, just change the spec file, then rerun gencsv.

Conventions:
1. Generate all such packages in subdirs of the "anydset" directory.
2. Add custom functionality in the file <packagename>_more.go in the same directory as the generated file.

Gencsv also generates test "main program" and a bash script to run the main program.
If "--Underscore" commandline parameter is "end", gencsv appends an underscore to column members.

Instance variables that are of foo.Bar type will result in an import of "foo" in the generated code

Gencsv generates code to store multiple hcsv instances in a map (PointerMap)
If an instance variable's config has "sort" in its hasindex field, code is generated to sort the PointerMap by that variable
(If the type is time.Time, it is sorted by UnixNano())

