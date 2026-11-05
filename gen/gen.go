//go:build ignore
// +build ignore

// gen.go regenerates gorm/gen query DAO from hand-written GORM models in
// gen/sqlmodel/ — not from database introspection.
//
// Run with: go run gen/gen.go
//
// gen/sqlmodel/models.go is the single source of truth for table columns,
// sizes, and indexes. gorm/gen emits type-safe DAO into gen/model/ and
// gen/query/. AutoMigrate in internal/db/sql.go consumes the same models
// to create matching tables.
package main

import (
	"fmt"

	"github.com/danchleo/workflow-go/gen/sqlmodel"
	"gorm.io/driver/sqlite"
	"gorm.io/gen"
	"gorm.io/gorm"
)

func main() {
	g := gen.NewGenerator(gen.Config{
		OutPath:      "gen/query",
		ModelPkgPath: "gen/model",
	})

	// Throwaway SQLite :memory: DB — gen only needs a valid gorm.DB handle
	// for Statement.Parse; ApplyBasic reads Go structs via reflection.
	d, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		panic(fmt.Sprintf("open sqlite :memory: %v", err))
	}
	g.UseDB(d)

	g.ApplyBasic(
		&sqlmodel.GWorkbookModel{},
		&sqlmodel.GWfDefModel{},
		&sqlmodel.GActDefModel{},
		&sqlmodel.GWfExModel{},
		&sqlmodel.GTaskExModel{},
		&sqlmodel.GActExModel{},
		&sqlmodel.GEnvModel{},
		&sqlmodel.GCronTriggerModel{},
		&sqlmodel.GScheduledJobModel{},
		&sqlmodel.GNamedLockModel{},
		&sqlmodel.GDynamicActionDefModel{},
		&sqlmodel.GCodeSourceModel{},
		&sqlmodel.GEventTriggerModel{},
		&sqlmodel.GResourceMemberModel{},
		&sqlmodel.GMetricsModel{},
		&sqlmodel.GDelayedCallModel{},
	)

	g.Execute()
	fmt.Println("gen done")
}
