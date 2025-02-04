#!/bin/bash
set -e

mongosh <<EOF

use $MONGO_INITDB_DATABASE

db.simple.insertMany( [
   {
      a: 'jelly',
      b: Int32(2000),
      c: Long(-13),
   },
   {
      a: 'butter',
      b: Int32(20021),
      c: Long(0),
   },
   {
      a: 'toast',
      b: Int32(2076),
      c: Long(2076),
   }
]);

db.primitives.insertMany( [
    {
        int32: Int32(42),
        int64: Long(23423),
        string: "hello",
        double: 1.22,
        boolean: true,
        binary: BinData(0, "asdfghjkl;"),
    },
    {
        int32: Int32(13),
        int64: Long(13),
        string: "hi",
        double: 1.23,
        boolean: false,
        binary: BinData(0, "qwerty"),
    },
    {
        int32: Int32(15),
        int64: Long(15),
        string: "bye",
        double: 1.24,
        boolean: false,
        binary: BinData(0, "ertwiou"),
    },
]);

db.missing.insertMany( [
    {
        int32: Int32(32),
        int64: Long(23423),
        string: "outer",
        double: 1.1,
        boolean: false,
    },
    {
        int32: Int32(64),
        double: 1.2,
        boolean: true,
        decimal: NumberDecimal("9823.1297"),
        binary: BinData(0, "qwerty"),
    },
])

db.uneven.insertMany( [
    {
        a: Int32(32),
        b: {foo: 32},
    },
    {
        a: Long(42),
        b: "b",
    },
])

db.nested.insertMany( [
    {
        arr: [],
        struct: {foo: 42},
        nested: [
            {one: 1},
            {two: 2},
            {three: 3},
        ]
    },
    {
        arr: [],
        struct: null,
    },
    {
        arr: [Int32(8)],
        struct: {
            foo: {
                bar: {
                    baz: ":)",
                }
            }
        },
    }
]);

db.datetime.insertMany( [
    {
        date: ISODate('2020-05-18T14:10:30.000Z'),
    },
    {
        date: ISODate(),
    }
]);

db.unsupported.insertOne( 
    {
        decimal: NumberDecimal("9823.1297"),
    }
);

EOF
