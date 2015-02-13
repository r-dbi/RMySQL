#ifndef __RMYSQL_MY_RESULT__
#define __RMYSQL_MY_RESULT__

#include <Rcpp.h>
#include <mysql.h>
#include <boost/noncopyable.hpp>
#include "MyTypes.h"

class MyResult : boost::noncopyable {
  MYSQL_STMT* pStatement_;
  MYSQL_RES* pSpec_;

  unsigned int nCols_;
  std::vector<MyFieldType> types_;
  std::vector<std::string> names_;

public:

  MyResult(MyConnectionPtr pConn, std::string sql) {
    // pConn->setCurrentResult(this);
    // Need to clean up on failure

    pStatement_ = mysql_stmt_init(pConn->conn());
    if (pStatement_ == NULL)
      Rcpp::stop("Failed to send query");

    if (mysql_stmt_prepare(pStatement_, sql.data(), sql.size()))
      throwError();

    pSpec_ = mysql_stmt_result_metadata(pStatement_);
    if (pSpec_ != NULL)
      cacheMetadata();



  }

  Rcpp::List columnInfo() {
    Rcpp::CharacterVector names(nCols_), types(nCols_);
    for (int i = 0; i < nCols_; i++) {
      names[i] = names_[i];
      types[i] = typeName(types_[i]);
    }

    Rcpp::List out = Rcpp::List::create(names, types);
    out.attr("row.names") = Rcpp::IntegerVector::create(NA_INTEGER, -nCols_);
    out.attr("class") = "data.frame";
    out.attr("names") = Rcpp::CharacterVector::create("name", "type");

    return out;
  }

  void throwError() {
    Rcpp::stop("%s [%i]",
      mysql_stmt_error(pStatement_),
      mysql_stmt_errno(pStatement_)
    );
  }

  ~MyResult() {
    try {
      mysql_stmt_close(pStatement_);
      mysql_free_result(pSpec_);
    } catch(...) {};
  }

private:

  void cacheMetadata() {
    nCols_ = mysql_num_fields(pSpec_);
    MYSQL_FIELD *fields = mysql_fetch_fields(pSpec_);

    for (int i = 0; i < nCols_; ++i) {
      names_.push_back(fields[i].name);
      types_.push_back(variableType(fields[i].type));
    }
  }

};

#endif
