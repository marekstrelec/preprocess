// Tool to convert raw CommonCrawl files into deduplicated files.
// Strips leading and trailing spaces.
// Removes document delimiter lines (those that begin with df6fa1abb58549287111ba8d776733e9).
// Removes duplicate lines.
// Removes any line that contains invalid UTF-8.
//
#include "util/fake_ofstream.hh"
#include "util/file_piece.hh"
#include "util/fixed_array.hh"
#include "util/murmur_hash.hh"
#include "util/probing_hash_table.hh"
#include "util/scoped.hh"
#include "util/utf8.hh"

#include <iostream>
#include <vector>

#include <stdint.h>
#include <string.h>

#include <ctype.h>
#include <stdlib.h>  // abort
#include <unistd.h>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

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
            << "\t-d [=NUM1:NUM2:NUM3]\tShard stdin into NUM1 shards. Shards from NUM2 to NUM3 will be outputted into individual files. Default is 1:0:0." << std::endl
            << "\t-o [=PATH]\tFile prefix of shards. The files will be named as file_prefix0 file_prefix1 etc." << std::endl
            << std::endl;
}

int main(int argc, char *argv[]) {
  char *avalue = NULL, *lvalue = NULL, *svalue = NULL, *ovalue = NULL;
  int shard_num = 1, shard_start = 0, shard_end = 0;
  bool use_shards = false;
  int c;
  while ((c = getopt (argc, argv, "ha:l:s:d:o:")) != -1) {
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
      case 'd':
        use_shards = true;

        {
          std::vector<std::string> splitted;
          boost::split(splitted, optarg, boost::is_any_of(":"));
          if  (splitted.size() != 3) {
            fprintf (stderr, "Illegal sharding format. Expecting NUM1:NUM2:NUM3.\n");
            return 1;
          }

          shard_num = boost::lexical_cast<unsigned>(splitted[0]);
          shard_start = boost::lexical_cast<unsigned>(splitted[1]);
          shard_end = boost::lexical_cast<unsigned>(splitted[2]);
        }

        break;
      case 'o':
        ovalue = optarg;
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
    // std::vector<Table> tables;
    Table tables[shard_end - shard_start + 1];
    StringPiece line;

    // Prepare Hash Table(s)
    if (lvalue != NULL) {
      for (int i = shard_start; i <= shard_end; ++i) {
        std::string load_path(lvalue);
        load_path.append(".");
        load_path.append(boost::lexical_cast<std::string>(i));
        tables[i - shard_start].~Table();
        new (&tables[i - shard_start]) Table(load_path.c_str());
      }
    }

    // Add each line of the file to the hash table
    if (avalue != NULL) {
      if (shard_num != 1) {
        fprintf (stderr, "-a can be used only with one shard.\n");
        return 1;
      }

      util::FilePiece removing(avalue);
      while (removing.ReadLineOrEOF(line)) {
        IsNewLine(tables[0], StripSpaces(line));
      }
    }

    // This is the beginning of a line that delimits documents in the raw files.
    const StringPiece remove_line("df6fa1abb58549287111ba8d776733e9");
    util::FakeOFStream out(1);
    util::FilePiece in(0, "stdin", &std::cerr);

    // prepare shards
    if (use_shards) {
      if (ovalue == NULL) {
        fprintf (stderr, "The sharding file prefix is not specified. Use -o.\n");
        return 1;
      }

      util::FixedArray<util::FakeOFStream> out(shard_end - shard_start + 1);
      for (int i = shard_start; i <= shard_end; ++i) {
        std::string output_path(ovalue);
        output_path.append(".");
        output_path.append(boost::lexical_cast<std::string>(i));
        out.push_back(util::CreateOrThrow(output_path.c_str()));
      }

      while (in.ReadLineOrEOF(line)) {
        int shard_idx = util::MurmurHashNative(line.data(), line.size(), 47849374332489ULL /* Be different from deduper */) % shard_num;
        if (shard_idx >= shard_start && shard_idx <= shard_end) {
          if (!starts_with(line, remove_line) && IsNewLine(tables[shard_idx - shard_start], line) && utf8::IsUTF8(line)) {
            out[shard_idx - shard_start] << line << '\n';
          }
        }
      }
    } else {
      while (in.ReadLineOrEOF(line)) {
        line = StripSpaces(line);
        // A line passes if:
        // It does not begin with the magic document delimiter.
        // Its 64-bit hash has not been seen before.
        // and it is valid UTF-8.
        if (!starts_with(line, remove_line) && IsNewLine(tables[0], line) && utf8::IsUTF8(line)) {
          out << line << '\n';
        }
      }
    }

    // Save Hash Table
    if (svalue != NULL) {
      for (int i = shard_start; i <= shard_end; ++i) {
        std::string save_path(svalue);
        save_path.append(".");
        save_path.append(boost::lexical_cast<std::string>(i));
        tables[i - shard_start].WriteToFile(save_path.c_str());
      }
    }
  }
  catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }
}
