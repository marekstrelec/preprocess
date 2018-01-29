// Tool to convert raw CommonCrawl files into deduplicated files.
// Strips leading and trailing spaces.
// Removes document delimiter lines (those that begin with df6fa1abb58549287111ba8d776733e9).
// Removes duplicate lines.
// Removes any line that contains invalid UTF-8.
//
#include "util/fake_ofstream.hh"
#include "util/file_piece.hh"
#include "util/murmur_hash.hh"
#include "util/probing_hash_table.hh"
#include "util/scoped.hh"
#include "util/utf8.hh"

#include <iostream>

#include <stdint.h>
#include <string.h>

#include <ctype.h>
#include <stdlib.h>  // abort
#include <unistd.h>

namespace {

// Hash table with 64-bit keys.
struct Entry {
  typedef uint64_t Key;
  uint64_t key;
  uint64_t GetKey() const { return key; }
  void SetKey(uint64_t to) { key = to; }
};

typedef util::AutoProbing<Entry, util::IdentityHash> Table;

// Use 64-bit MurmurHash in the hash table.
bool IsNewLine(Table &table, StringPiece l) {
  Table::MutableIterator it;
  Entry entry;
  entry.key = util::MurmurHashNative(l.data(), l.size(), 1);
  return !table.FindOrInsert(entry, it);
}

// Remove leading and trailing space characters.
StringPiece StripSpaces(StringPiece ret) {
  while (ret.size() && util::kSpaces[static_cast<unsigned char>(*ret.data())]) {
    ret = StringPiece(ret.data() + 1, ret.size() - 1);
  }
  while (ret.size() && util::kSpaces[static_cast<unsigned char>(ret.data()[ret.size() - 1])]) {
    ret = StringPiece(ret.data(), ret.size() - 1);
  }
  return ret;
}


} // namespace


void show_help() {
  std::cerr << "Usage: " << std::endl
            << "\t-h\t\tDisplay this help and exit" << std::endl
            << "\t-t [=PATH]\tInput hash table file" << std::endl
            << std::endl;
}

int main(int argc, char *argv[]) {
  char *tvalue = NULL;
  int c;
  while ((c = getopt (argc, argv, "ht:")) != -1) {
    switch (c) {
      case 'h':
        show_help();
        return 1;
      case 't':
        tvalue = optarg;
        break;
      case '?':
        if (optopt == 'r' || optopt == 't' || optopt == 'o')
          fprintf (stderr, "Option -%c requires an argument.\n", optopt);
        else if (isprint (optopt))
          fprintf (stderr, "Unknown option `-%c'.\n", optopt);
        else
          fprintf (stderr, "Unknown option character `\\x%x'.\n", optopt);
        show_help();
        return 1;
      default:
        abort ();
    }
  }

  if (tvalue == NULL) {
    std::cerr << "tvalue cannot be empty!" << std::endl;
    return 1;
  }

  try {
    std::size_t allocated_;
    std::size_t entries_;
    std::size_t threshold_;
    int fd = util::OpenReadOrThrow(tvalue);
    util::ReadOrThrow(fd, &allocated_, sizeof(std::size_t));
    util::ReadOrThrow(fd, &entries_, sizeof(std::size_t));
    util::ReadOrThrow(fd, &threshold_, sizeof(std::size_t));

    std::cout << "allocated_: " << allocated_ << std::endl;
    std::cout << "entries_: " << entries_ << std::endl;
    std::cout << "threshold_: " << threshold_ << std::endl;
  }
  catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }
}
