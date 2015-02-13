#include <Rcpp.h>
#include "RMySQL_types.h"
using namespace Rcpp;

// [[Rcpp::export]]
XPtr<MyResult> result_create(XPtr<MyConnectionPtr> con, std::string sql) {
  MyResult* res = new MyResult(*con, sql);
  return XPtr<MyResult>(res, true);
}

// [[Rcpp::export]]
List result_column_info(XPtr<MyResult> rs) {
  return rs->columnInfo();
}
