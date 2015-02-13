#ifndef __RMYSQL_MY_RESULT__
#define __RMYSQL_MY_RESULT__

#include <Rcpp.h>
#include <mysql.h>
#include <boost/noncopyable.hpp>
#include "MyBinding.h"
#include "MyTypes.h"
#include "MyUtils.h"

class MyResult : boost::noncopyable {
  MyConnectionPtr pConn_;
  MYSQL_STMT* pStatement_;
  MYSQL_RES* pSpec_;
  int rowsAffected_, rowsFetched_;

  unsigned int nCols_, nParams_;
  bool bound_, complete_;

  std::vector<MyFieldType> types_;
  std::vector<std::string> names_;
  MyRow* bindingRow_;

public:

  MyResult(MyConnectionPtr pConn, std::string sql):
    pConn_(pConn),
    rowsFetched_(0)
  {
    pConn_->setCurrentResult(this);

    try {
      pStatement_ = mysql_stmt_init(pConn->conn());
      if (pStatement_ == NULL)
        Rcpp::stop("Failed to send query");

      if (mysql_stmt_prepare(pStatement_, sql.data(), sql.size()) != 0)
        throwError();

      pSpec_ = mysql_stmt_result_metadata(pStatement_);
      if (pSpec_ != NULL) {
        cacheMetadata();
        rowsAffected_ = 0;
        complete_ = false;

        bindingRow_ = new MyRow(pStatement_, types_);
      } else {
        rowsAffected_ = mysql_stmt_affected_rows(pStatement_);
        complete_ = true;
      }

      if (mysql_stmt_execute(pStatement_) != 0)
        throwError();
    } catch (...) {
      pConn_->setCurrentResult(NULL);
      throw;
    }

    nParams_ = mysql_stmt_param_count(pStatement_);
    bound_ = (nParams_ == 0);

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

  bool fetchRow() {
    int result = mysql_stmt_fetch(pStatement_);

    switch(result) {
    // We expect truncation whenever there's a string or blob
    case MYSQL_DATA_TRUNCATED:
    case 0:
      rowsFetched_++;
      return true;
    case 1:
      throwError();
    case MYSQL_NO_DATA:
      return false;
    }
    return false;
  }

  Rcpp::List fetch(int n_max = -1) {
    if (!bound_)
      Rcpp::stop("Query needs to be bound before fetching");
    if (!active())
      Rcpp::stop("Inactive result set");

    int n = (n_max < 0) ? 100 : n_max;
    Rcpp::List out = dfCreate(types_, names_, n);
    if (n == 0)
      return out;

    int i = 0;

    while(fetchRow()) {
      if (i >= n) {
        if (n_max < 0) {
          n *= 2;
          out = dfResize(out, n);
        } else {
          break;
        }
      }

      for (int j = 0; j < nCols_; ++j) {
        // Rcpp::Rcout << i << "," << j << "\n";
        bindingRow_->setListValue(out[j], i, j);
      }

      ++i;
      if (i % 1000 == 0)
        Rcpp::checkUserInterrupt();
    }

    // Trim back to what we actually used
    if (i < n) {
      out = dfResize(out, i);
    }

    return out;
  }

  bool active() {
    return pConn_->isCurrentResult(this);
  }

  void throwError() {
    Rcpp::stop("%s [%i]",
      mysql_stmt_error(pStatement_),
      mysql_stmt_errno(pStatement_)
    );
  }

  void close() {
    if (pStatement_ != NULL) {
      mysql_stmt_close(pStatement_);
      pStatement_ = NULL;
    }
    if (pSpec_ != NULL) {
      mysql_free_result(pSpec_);
      pSpec_ = NULL;
    }
  }

  ~MyResult() {
    try {
      pConn_->setCurrentResult(NULL);
      close();
    } catch(...) {};
  }

private:

  void cacheMetadata() {
    nCols_ = mysql_num_fields(pSpec_);
    MYSQL_FIELD *fields = mysql_fetch_fields(pSpec_);

    for (int i = 0; i < nCols_; ++i) {
      names_.push_back(fields[i].name);

      MyFieldType type = variableType(fields[i].type);
      types_.push_back(type);
    }
  }

};

#endif
