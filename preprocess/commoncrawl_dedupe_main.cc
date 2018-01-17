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
            << "\t-a [=PATH]\tAdd each line of the file to the hash table" << std::endl
            << "\t-l [=PATH]\tLoad the hash table from this file" << std::endl
            << "\t-s [=PATH]\tSave the hash table to this file" << std::endl
            << std::endl;
}

int main(int argc, char *argv[]) {
  char *avalue = NULL;
  char *lvalue = NULL;
  char *svalue = NULL;
  int c;
  while ((c = getopt (argc, argv, "ha:l:s:")) != -1) {
    switch (c) {
      case 'h':
        show_help();
        return 1;
      case 'a':
        avalue = optarg;
        break;
      case 'l':
        lvalue = optarg;
        break;
      case 's':
        svalue = optarg;
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

  try {
    Table table;
    StringPiece l;

    // Load Hash Table
    if (lvalue != NULL) {
      table.~Table();
      new (&table) Table(lvalue);
    }

    // Add each line of the file to the hash table
    if (avalue != NULL) {
      util::FilePiece removing(avalue);
      while (removing.ReadLineOrEOF(l)) {
        IsNewLine(table, StripSpaces(l));
      }
    }

    // This is the beginning of a line that delimits documents in the raw files.
    const StringPiece remove_line("df6fa1abb58549287111ba8d776733e9");
    util::FakeOFStream out(1);
    util::FilePiece in(0, "stdin", &std::cerr);
    while (in.ReadLineOrEOF(l)) {
      l = StripSpaces(l);
      // A line passes if:
      // It does not begin with the magic document delimiter.
      // Its 64-bit hash has not been seen before.
      // and it is valid UTF-8.
      if (!starts_with(l, remove_line) && IsNewLine(table, l) && utf8::IsUTF8(l)) {
        out << l << '\n';
      }
    }

    // Save Hash Table
    if (svalue != NULL) {
      table.WriteToFile(svalue);
    }
  }
  catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }
}
