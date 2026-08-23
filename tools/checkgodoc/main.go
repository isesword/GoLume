// Command checkgodoc verifies GoDoc conventions for exported production API.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
)

type packageDocs struct {
	name       string
	hasPackage bool
	hasCommand bool
}

func exported(name string) bool {
	if name == "" {
		return false
	}
	return unicode.IsUpper([]rune(name)[0])
}

func commentText(group *ast.CommentGroup) string {
	if group == nil {
		return ""
	}
	return strings.TrimSpace(group.Text())
}

func startsWithName(group *ast.CommentGroup, name string) bool {
	text := commentText(group)
	return text == name || strings.HasPrefix(text, name+" ") || strings.HasPrefix(text, name+".") || strings.HasPrefix(text, name+",")
}

func shouldSkipDir(root, path string) bool {
	if path == root {
		return false
	}
	base := filepath.Base(path)
	if strings.HasPrefix(base, ".") || base == "testdata" || base == "validation" {
		return true
	}
	return base == "offline-poc" && filepath.Base(filepath.Dir(path)) == "tools"
}

func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}

	fset := token.NewFileSet()
	issues := 0
	packages := make(map[string]*packageDocs)

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if shouldSkipDir(root, path) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		f, parseErr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if parseErr != nil {
			return parseErr
		}

		dir := filepath.Dir(path)
		info := packages[dir]
		if info == nil {
			info = &packageDocs{name: f.Name.Name}
			packages[dir] = info
		}
		if f.Doc != nil {
			text := commentText(f.Doc)
			if strings.HasPrefix(text, "Package "+f.Name.Name+" ") || text == "Package "+f.Name.Name+"." {
				info.hasPackage = true
			}
			if f.Name.Name == "main" && strings.HasPrefix(text, "Command ") {
				info.hasCommand = true
			}
		}

		for _, decl := range f.Decls {
			switch x := decl.(type) {
			case *ast.GenDecl:
				for _, spec := range x.Specs {
					switch y := spec.(type) {
					case *ast.TypeSpec:
						if !exported(y.Name.Name) {
							continue
						}
						doc := y.Doc
						if doc == nil {
							doc = x.Doc
						}
						if doc == nil {
							fmt.Printf("%s:%d: exported type %s is missing GoDoc\n", path, fset.Position(y.Pos()).Line, y.Name.Name)
							issues++
						} else if !startsWithName(doc, y.Name.Name) {
							fmt.Printf("%s:%d: GoDoc for type %s must start with %q\n", path, fset.Position(y.Pos()).Line, y.Name.Name, y.Name.Name)
							issues++
						}
					case *ast.ValueSpec:
						// Grouped constants and variables commonly document the group rather
						// than repeat every exported identifier. Require documentation, but
						// do not impose a per-name prefix on grouped declarations.
						for _, name := range y.Names {
							if exported(name.Name) && x.Doc == nil && y.Doc == nil {
								fmt.Printf("%s:%d: exported value %s is missing GoDoc\n", path, fset.Position(name.Pos()).Line, name.Name)
								issues++
							}
						}
					}
				}
			case *ast.FuncDecl:
				if !exported(x.Name.Name) {
					continue
				}
				if x.Doc == nil {
					fmt.Printf("%s:%d: exported function %s is missing GoDoc\n", path, fset.Position(x.Pos()).Line, x.Name.Name)
					issues++
				} else if !startsWithName(x.Doc, x.Name.Name) {
					fmt.Printf("%s:%d: GoDoc for function %s must start with %q\n", path, fset.Position(x.Pos()).Line, x.Name.Name, x.Name.Name)
					issues++
				}
			}
		}
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	dirs := make([]string, 0, len(packages))
	for dir := range packages {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	for _, dir := range dirs {
		p := packages[dir]
		if p.name == "main" {
			if !p.hasCommand && !p.hasPackage {
				fmt.Printf("%s: package main is missing a package or Command doc comment\n", dir)
				issues++
			}
			continue
		}
		if !p.hasPackage {
			fmt.Printf("%s: package %s is missing a \"Package %s ...\" doc comment\n", dir, p.name, p.name)
			issues++
		}
	}

	if issues != 0 {
		fmt.Fprintf(os.Stderr, "%d GoDoc convention issue(s) found\n", issues)
		os.Exit(1)
	}
	fmt.Println("GoDoc check: PASS (package docs and exported API comments follow naming conventions)")
}
