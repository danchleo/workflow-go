//go:build ignore
// +build ignore

// gen.go regenerates gorm/gen query DAO for all Python-parity tables.
// Run with: go run gen/gen.go
//
// Uses the actual gorm models from internal/db/sql.go (gWorkbook, gNamedLock, etc.)
// because those are the actual DB-schema models AutoMigrate uses.
// gen introspects them and produces type-safe DAO in gen/query/.
package main

import (
	"fmt"

	"gorm.io/driver/mysql"
	"gorm.io/gen"
	"gorm.io/gorm"
)

const dsn = "root:Mypass_802@tcp(localhost:3306)/workflow_go?charset=utf8mb4&parseTime=True&loc=Local"

func main() {
	g := gen.NewGenerator(gen.Config{
		OutPath:      "gen/query",
		ModelPkgPath: "gen/model",
	})

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(fmt.Sprintf("connect mysql: %v", err))
	}
	g.UseDB(db)

	// Generate ALL tables — gen introspects actual MySQL schema,
	// so the generated model matches AutoMigrate exactly.
	g.ApplyBasic(g.GenerateAllTable()...)

	g.Execute()
	fmt.Println("gen done")
}
