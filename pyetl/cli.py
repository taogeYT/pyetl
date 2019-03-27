import shutil
import argparse
import os
file_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(os.path.join(file_dir, "templates"), 'project')


def get_args():
    parser = argparse.ArgumentParser(description='新建项目')
    parser.add_argument('-b', '--build', help='项目名称', default=None, type=str)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    if args.build:
        if os.path.exists(args.build):
            print("Error: %s has exist" % args.build)
        else:
            shutil.copytree(src_dir, args.build)
            print("build a new project: %s" % args.build)
    else:
        print("project name need")
