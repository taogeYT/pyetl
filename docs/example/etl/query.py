from py_db import connection
import json
import argparse


def get_args():
    parser = argparse.ArgumentParser(description="query task")
    parser.add_argument('--table', help="table name")
    args = parser.parse_args()
    return args


def main(args):
    sql = "select * from task"
    if args.table:
        sql += " where dst_table='%s'" % args.table
    with connection("task.db", driver='sqlite3') as db:
        print(sql)
        rs = db.query(sql)
        print(json.dumps(rs, indent=1))


if __name__ == "__main__":
    args = get_args()
    main(args)
