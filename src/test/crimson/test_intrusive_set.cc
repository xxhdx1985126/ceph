// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include <iostream>
#include <vector>
#include <boost/intrusive/set.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

struct comparer;

class TestObj : public boost::intrusive_ref_counter<
  TestObj, boost::thread_unsafe_counter> {
public:
  TestObj(int x) : x(x) {};
  boost::intrusive::set_member_hook<> hook;
  using test_member_options = boost::intrusive::member_hook<
    TestObj,
    boost::intrusive::set_member_hook<>,
    &TestObj::hook>;
private:
  int x;
  friend struct comparer;
  friend bool operator< (const TestObj& a, const TestObj& b) {
    return a.x < b.x;
  }
  friend bool operator> (const TestObj& a, const TestObj& b) {
    return a.x > b.x;
  }
  friend bool operator== (const TestObj& a, const TestObj& b) {
    return a.x == b.x;
  }
};

struct comparer {
  bool operator()(int l, const TestObj& r) const {
    return l < r.x;
  }
  bool operator()(const TestObj& l, int r) const {
    return l.x < r;
  }
};

int main() {
  boost::intrusive::set<TestObj, TestObj::test_member_options> test_set;
  std::vector<TestObj> orig_set;
  for (int i = 0; i < 10000; i++) {
    orig_set.emplace_back(i);
  }
  for (int i = 0; i < 10000; i++) {
    test_set.insert(orig_set[i]);
  }
  std::cout << "try to find i=0: " << (test_set.find(1, comparer()) == test_set.end()) << std::endl;
  test_set.clear();
  orig_set.clear();
  return 0;
}
