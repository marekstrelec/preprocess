#ifndef LM_FILTER_H__
#define LM_FILTER_H__
/* Filter an ARPA language model to only contain words found in a vocabulary
 * plus tags with < and > around them.  
 */

#include "util/multi_intersection.hh"
#include "util/string_piece.hh"
#include "util/tokenize_piece.hh"

#include <boost/noncopyable.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/unordered/unordered_map.hpp>
#include <boost/unordered/unordered_set.hpp>

#include <set>
#include <string>
#include <vector>

namespace lm {

class PhraseSubstrings;

/* Is this a special tag like <s> or <UNK>?  This actually includes anything
 * surrounded with < and >, which most tokenizers separate for real words, so
 * this should not catch real words as it looks at a single token.   
 */
inline bool IsTag(const StringPiece &value) {
  // The parser should never give an empty string.
  assert(!value.empty());
  return (value.data()[0] == '<' && value.data()[value.size() - 1] == '>');
}

class SingleBinary {
  public:
    typedef boost::unordered_set<std::string> Words;

    explicit SingleBinary(const Words &vocab) : vocab_(vocab) {}

    template <class Iterator> bool PassNGram(const Iterator &begin, const Iterator &end) {
      for (Iterator i = begin; i != end; ++i) {
        if (IsTag(*i)) continue;
        if (FindStringPiece(vocab_, *i) == vocab_.end()) return false;
      }
      return true;
    }

  private:
    const Words &vocab_;
};

class UnionBinary {
  public:
    typedef boost::unordered_map<std::string, std::vector<unsigned int> > Words;

    explicit UnionBinary(const Words &vocabs) : vocabs_(vocabs) {}

    template <class Iterator> bool PassNGram(const Iterator &begin, const Iterator &end) {
      sets_.clear();

      for (Iterator i(begin); i != end; ++i) {
        if (IsTag(*i)) continue;
        Words::const_iterator found(FindStringPiece(vocabs_, *i));
        if (vocabs_.end() == found) return false;
        sets_.push_back(boost::iterator_range<const unsigned int*>(&*found->second.begin(), &*found->second.end()));
      }
      return (sets_.empty() || util::FirstIntersection(sets_));
    }

  private:
    const Words &vocabs_;

    std::vector<boost::iterator_range<const unsigned int*> > sets_;
};

template <class OutputT> class MultipleOutputVocabFilter {
  public:
    typedef OutputT Output;
    typedef boost::unordered_map<std::string, std::vector<unsigned int> > Words;

    MultipleOutputVocabFilter(const Words &vocabs, Output &output) : vocabs_(vocabs), output_(output) {}

    Output &GetOutput() { return output_; }

  private:
    // Callback from AllIntersection that does AddNGram.
    class Callback {
      public:
        Callback(Output &out, const std::string &line) : out_(out), line_(line) {}

        void operator()(unsigned int index) {
          out_.SingleAddNGram(index, line_);
        }

      private:
        Output &out_;
        const std::string &line_;
    };

  public:
    template <class Iterator> void AddNGram(const Iterator &begin, const Iterator &end, const std::string &line) {
      sets_.clear();
      for (Iterator i(begin); i != end; ++i) {
        if (IsTag(*i)) continue;
        Words::const_iterator found(FindStringPiece(vocabs_, *i));
        if (vocabs_.end() == found) return;
        sets_.push_back(boost::iterator_range<const unsigned int*>(&*found->second.begin(), &*found->second.end()));
      }
      if (sets_.empty()) {
        output_.AddNGram(line);
        return;
      }

      Callback cb(output_, line);
      util::AllIntersection(sets_, cb);
    }

  private:
    const Words &vocabs_;

    Output &output_;

    std::vector<boost::iterator_range<const unsigned int*> > sets_;
};

namespace detail {
const StringPiece kEndSentence("</s>");
} // namespace detail

class PhraseBinary {
  public:
    explicit PhraseBinary(const PhraseSubstrings &substrings) : substrings_(substrings) {}

    template <class Iterator> bool PassNGram(const Iterator &begin, const Iterator &end) {
      MakePhraseHashes(begin, end);
      return hashes_.empty() || Evaluate<true>();
    }

  protected:
    template <class Iterator> void MakePhraseHashes(Iterator i, const Iterator &end) {
      swap(hashes_, pre_hashes_);
      hashes_.clear();
      if (i == end) return;
      // TODO: check strict phrase boundaries after <s> and before </s>.  For now, just skip tags.  
      if (IsTag(*i)) ++i;
      boost::hash<StringPiece> hasher;
      for (; i != end && (*i != detail::kEndSentence); ++i) {
        hashes_.push_back(hasher(*i));
      }
    }

    template <bool ExitEarly> bool Evaluate();

    const std::set<unsigned int> &Matches() const { return matches_; }

    bool HashesEmpty() const { return hashes_.empty(); }

  private:
    const PhraseSubstrings &substrings_;
    // Vector of hash codes for each string in the n-gram.  
    // pre_hashes_ is the vector for the previous n-gram.  
    std::vector<size_t> pre_hashes_, hashes_;

    // Reach vector.  Used in evaluate.  Keeps state if previous n-gram is the same.  
    std::vector<std::set<unsigned int> > reach_;

    // Matches come out here.  
    std::set<unsigned int> matches_;
};

template <class OutputT> class MultipleOutputPhraseFilter : public PhraseBinary {
  public:
    typedef OutputT Output;
    
    explicit MultipleOutputPhraseFilter(const PhraseSubstrings &substrings, Output &output) : PhraseBinary(substrings), output_(output) {}

    Output &GetOutput() { return output_; }

  public:
    template <class Iterator> void AddNGram(const Iterator &begin, const Iterator &end, const std::string &line) {
      MakePhraseHashes(begin, end);
      if (HashesEmpty()) {
        output_.AddNGram(line);
        return;
      }
      Evaluate<false>();
      for (std::set<unsigned int>::const_iterator i = Matches().begin(); i != Matches().end(); ++i)
        output_.SingleAddNGram(*i, line);
    }

  private:
    Output &output_;
};

template <class Binary, class OutputT> class SingleOutputFilter {
  public:
    typedef OutputT Output;

    // Binary modles are just references (and a set) and it makes the API cleaner to copy them.  
    SingleOutputFilter(Binary binary, Output &output) : binary_(binary), output_(output) {}

    Output &GetOutput() { return output_; }

    template <class Iterator> void AddNGram(const Iterator &begin, const Iterator &end, const std::string &line) {
      if (binary_.PassNGram(begin, end))
        output_.AddNGram(line);
    }

  private:
    Binary binary_;
    Output &output_;
};

/* Wrap another filter to pay attention only to context words */
template <class FilterT> class ContextFilter {
  public:
    typedef FilterT Filter;
    typedef typename Filter::Output Output;

    ContextFilter(Filter &backend) : backend_(backend) {}

    Output &GetOutput() { return backend_.GetOutput(); }

    template <class Iterator> void AddNGram(const Iterator &begin, const Iterator &end, const std::string &line) {
      assert(begin != end);
      // TODO: this copy could be avoided by a lookahead iterator.
      pieces_.clear();
      std::copy(begin, end, std::back_insert_iterator<std::vector<StringPiece> >(pieces_));
      backend_.AddNGram(pieces_.begin(), pieces_.end() - 1, line);
    }

  private:
    std::vector<StringPiece> pieces_;

    Filter &backend_;
};

} // namespace lm

#endif // LM_FILTER_H__
