#!/bin/bash

pkgdir=./_output/go/src/genpkgs

function genOne()
{
  local pkg=$1

  mkdir -p $pkgdir/$pkg
  mkdir -p _doit _test
  echo ../gencsv --Cfg $pkg.cfg --TestMain _test/test_$pkg.go --TestBash _doit/doit_$pkg.bash --Ofile $pkgdir/$pkg/$pkg.go --Pkg $pkg --Underscore end
       ../gencsv --Cfg $pkg.cfg --TestMain _test/test_$pkg.go --TestBash _doit/doit_$pkg.bash --Ofile $pkgdir/$pkg/$pkg.go --Pkg $pkg --Underscore end

  echo "if it worked, please consider:"
  echo "           git add $pkgdir/$pkg $pkg.cfg test/test_$pkg.go doit/doit_$pkg.bash"
}

genOne foo1	# simple case (has one single key index)
genOne foo2	# as above, but with an instance variable
genOne foo3	# one multikey index
genOne foo4	# 2 multikey indexes and one singlekey index
genOne foo5	# adds some hidden variables




