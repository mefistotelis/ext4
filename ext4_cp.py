#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" Copy files from ext4 disk image.

 This script is a version of cp command which accepts ext4 disk image as source.
"""
# Copyright (C) 2021 Mefistotelis <mefistotelis@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

__version__ = "0.0.1"
__author__ = "Mefistotelis"
__license__ = "GPL"

import ext4

import os
import sys
import argparse

def extract(inode, path, rel_path, file_name, file_type, args):
    """ Callback function for extracting files.

    Returns true if current inode is expected to be handled recursively as folder.
    """
    dst_fpath = []
    if args.directory != "":
        dst_fpath.append(args.directory)
    if rel_path != "":
        dst_fpath.append(rel_path)
    dst_fpath.append(file_name)
    dst_fpath = "/".join(dst_fpath)
    if file_type == ext4.InodeType.FILE:
        if args.verbose > 1:
            print(f"{rel_path:s}/{file_name:s}")
        reader = inode.open_read() # Either ext4.BlockReader or io.BytesIO
        with open(dst_fpath, "wb") as dst_file:
            dst_file.write(reader.read())
    elif file_type == ext4.InodeType.DIRECTORY:
        if file_name in (".","..",):
            pass
        elif not args.recursive:
            if args.verbose > 0:
                print(f"{args.imgfname:s}: -R not specified; omitting directory '{rel_path:s}/{file_name:s}'")
        else:
            if args.verbose > 1:
                print(f"{rel_path:s}/{file_name:s}")
            os.mkdir(dst_fpath)
            return True
    elif file_type == ext4.InodeType.CHARACTER_DEVICE:
        if args.verbose > 1:
            print(f"{rel_path:s}/{file_name:s}")
        with open(dst_fpath, "wb") as dst_file:
            pass
    elif file_type == ext4.InodeType.BLOCK_DEVICE:
        if args.verbose > 1:
            print(f"{rel_path:s}/{file_name:s}")
        with open(dst_fpath, "wb") as dst_file:
            pass
    elif file_type == ext4.InodeType.FIFO:
        if args.verbose > 1:
            print(f"{rel_path:s}/{file_name:s}")
        with open(dst_fpath, "wb") as dst_file:
            pass
    elif file_type == ext4.InodeType.SOCKET:
        if args.verbose > 1:
            print(f"{rel_path:s}/{file_name:s}")
        with open(dst_fpath, "wb") as dst_file:
            pass
    elif file_type == ext4.InodeType.SYMBOLIC_LINK:
        if args.verbose > 1:
            print(f"{rel_path:s}/{file_name:s}")
        reader = inode.open_read()
        symlink_fpath = reader.read().decode("utf8")
        os.symlink(symlink_fpath, dst_fpath)
    return False


def for_all_entries_do(inode, full_path, part_path, do_func, args):
    """ Executes given function for all entries within the inode, recursively.
    """
    for file_name, inode_idx, file_type in inode.open_dir():
        sub_inode = inode.volume.get_inode(inode_idx)
        go_deeper = do_func(sub_inode, full_path, part_path, file_name, file_type, args)
        if go_deeper:
            for_all_entries_do(sub_inode, full_path+"/"+file_name, part_path+"/"+file_name, do_func, args)

def for_path_do(inode, current_path, target_path, do_func, args):
    """ Executes given function for entries within an inode at given path, recursively.
        @param inode initial inode
        @param current_path path of the initial inode
        @param target_path the path to target node, relative to initial inode
        @param do_func function performing an action on each node
        @param args arguments array to be transferred to do_func
    """
    relative_path = target_path.split("/")
    if len(relative_path) > 1:
        parent_inode = inode.get_inode(*relative_path[:-1])
        sub_path = current_path + "/" + "/".join(relative_path[:-1])
    else:
        parent_inode = inode
        sub_path = current_path

    if relative_path[-1] == ".":
        sub_inode = parent_inode
        sub_path = current_path
        for_all_entries_do(sub_inode, sub_path, "", do_func, args)
        return

    sub_inode = None
    for file_name, inode_idx, file_type in parent_inode.open_dir():
        if file_name != relative_path[-1]:
            continue
        sub_inode = inode.volume.get_inode(inode_idx)
        break
    if sub_inode is None:
        file_name = relative_path[-1]
        raise FileNotFoundError(f"'{file_name:s}' not found in '{sub_path:s}'.")
    else:
        go_deeper = do_func(sub_inode, sub_path, "", file_name, file_type, args)
        if go_deeper:
            for_all_entries_do(sub_inode, sub_path+"/"+file_name, file_name, do_func, args)
    return

def main():
    """ Main executable function.

    Its task is to parse command line options and call a function which performs requested command.
    """
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument('source', metavar='SOURCE', type=str, nargs='+',
            help="the ext4 image file and source file name to read from, IMAGE:FILE or IMAGE:. to include all")

    parser.add_argument('directory', metavar='DIRECTORY', type=str,
            help="the target directory to store the copied files")

    parser.add_argument('-R', '--recursive', action='store_true',
            help="copy the SOURCE folders recursively")

    parser.add_argument('-v', '--verbose', action='count', default=0,
            help="increases verbosity level; max level is set by -vvv")

    parser.add_argument('--version', action='version', version="%(prog)s {version} by {author}"
              .format(version=__version__,author=__author__),
            help="display version information and exit")

    args = parser.parse_args()

    args.imgfname = args.source[0].split(":", 1)[0]
    args.src_fnames = [ ]
    for fname in args.source:
        fnsplit = fname.split(":", 1)
        if fnsplit[0] != args.imgfname:
            raise ValueError(f"Single command can only extract from one image, not '{fnsplit[0]:s}'.")
        src_fname = fnsplit[1] if len(fnsplit) > 1 else "."
        if src_fname.endswith("/"): src_fname = src_fname[:-1]
        args.src_fnames += [ src_fname ]

    if args.directory.endswith("/"): args.directory = args.directory[:-1]

    imgfile = open(args.imgfname, "rb")
    volume = ext4.Volume(imgfile, offset = 0)

    if args.verbose > 0:
        print(f"{args.imgfname:s}, Volume {volume.uuid:s} has block size {volume.block_size:d}")

    for src_fname in args.src_fnames:
        for_path_do(volume.root, "", src_fname, extract, args)

if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print("Error: "+str(ex))
        #raise
        sys.exit(10)
