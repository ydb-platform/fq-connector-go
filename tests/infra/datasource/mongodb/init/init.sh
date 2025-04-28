#!/bin/bash
set -e

mongosh <<EOF

use $MONGO_INITDB_DATABASE

db.simple.insertMany( [
   {
      _id: Int32(0),
      a: 'jelly',
      b: Int32(2000),
      c: Long(13),
   },
   {
      _id: Int32(1),
      a: 'butter',
      b: Int32(-20021),
      c: Long(0),
   },
   {
      _id: Int32(2),
      a: 'toast',
      b: Int32(2076),
      c: Long(2076),
   }
]);

db.primitives.insertMany( [
    {
        _id: Int32(0),
        int32: Int32(42),
        int64: Long(23423),
        string: "hello",
        double: 1.22,
        boolean: true,
        binary: Binary.createFromHexString("aaaa"),
        objectid: ObjectId('171e75500ecde1c75c59139e'),
    },
    {
        _id: Int32(1),
        int32: Int32(13),
        int64: Long(13),
        string: "hi",
        double: 1.23,
        boolean: false,
        binary: Binary.createFromHexString("abab"),
        objectid: ObjectId('271e75500ecde1c75c59139e'),
    },
    {
        _id: Int32(2),
        int32: Int32(15),
        int64: Long(15),
        string: "bye",
        double: 1.24,
        boolean: false,
        binary: Binary.createFromHexString("acac"),
        objectid: ObjectId('371e75500ecde1c75c59139e'),
    },
]);

db.missing.insertMany( [
    {
        _id: Int32(0),
        int32: Int32(64),
        int64: Long(23423),
        string: "outer",
        double: 1.1,
        binary: Binary.createFromHexString("abcd"),
        boolean: false,
        objectid: ObjectId('171e75500ecde1c75c59139e'),
    },
    {
        _id: Int32(1),
        int32: Int32(32),
        double: 1.2,
        boolean: true,
        decimal: NumberDecimal("9823.1297"),
    },
    {
        _id: Int32(2),
    },
])

db.uneven.insertMany( [
    {
        _id: Int32(0),
        a: Int32(32),
        b: {foo: 32},
        c: "bye",
        d: 1.24,
        e: false,
    },
    {
        _id: Int32(1),
        a: Long(42),
        b: "b",
        c: Binary.createFromHexString("acac"),
        d: ObjectId('371e75500ecde1c75c59139e'),
        e: Int32(0),
    },
])

db.nested.insertMany( [
    {
        _id: Int32(0),
        arr: [],
        struct: {foo: 42},
        nested: [
            {one: 1},
            {two: 2},
            {three: 3},
        ]
    },
    {
        _id: Int32(1),
        arr: [],
        struct: null,
    },
    {
        _id: Int32(2),
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
        _id: Int32(0),
        date: ISODate('2020-05-18T14:10:30.000Z'),
        datestr: ISODate('2025-05-18T14:10:30.000Z'),
    },
    {
        _id: Int32(1),
        date: ISODate('2025-05-01T11:10:30.000Z'),
        datestr: "not a date",
    }
]);

db.unsupported.insertOne( 
    {
        _id: Int32(2202),
        decimal: NumberDecimal("9823.1297"),
    }
);

db.similar.insertMany( [
    {
        _id: Int32(0),
        a: Int32(1),
        b: "hello",
    },
    {
        _id: Int32(1),
        a: Int32(1),
        b: "hi",
    },
    {
        _id: Int32(2),
        a: Int32(2),
        b: "hello",
    },
    {
        _id: Int32(3),
        a: Int32(2),
        b: "one",
    },
    {
        _id: Int32(4),
        a: Int32(2),
        b: "two",
    },
    {
        _id: Int32(5),
        a: Int32(6),
        b: "three",
    },
    {
        _id: Int32(6),
        a: Int32(9),
        b: "four",
        c: "surprise",
    },
]);

EOF
