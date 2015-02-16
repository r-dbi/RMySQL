#ifndef __RMYSQL_MY_BINDING__
#define __RMYSQL_MY_BINDING__

#include <Rcpp.h>
#include <mysql.h>
#include <boost/noncopyable.hpp>
#include "MyTypes.h"
#include <ctime>

class MyBinding : boost::noncopyable {
  MYSQL_STMT* pStatement_;

  int p_;
  std::vector<MYSQL_BIND> bindings_;
  std::vector<my_bool> isNull_;
  std::vector<MyFieldType> types_;

public:
  MyBinding(MYSQL_STMT* pStatement):
    pStatement_(pStatement)
  {
    p_ = mysql_stmt_param_count(pStatement_);

    bindings_.resize(p_);
    types_.resize(p_);
    isNull_.resize(p_);
  }

  void initBinding(Rcpp::List params) {
    if (p_ != params.size()) {
      Rcpp::stop("Number of params don't match (%i vs %i)", p_, params.size());
    }

    for (int j = 0; j < p_; ++j) {
      MyFieldType type = variableType(params[j]);
      types_[j] = type;

      switch(type) {
      case MY_LGL:       bindingUpdate(j, MYSQL_TYPE_TINY, 1); break;
      case MY_INT32:     bindingUpdate(j, MYSQL_TYPE_INT24, 4); break;
      case MY_DBL:       bindingUpdate(j, MYSQL_TYPE_DOUBLE, 8); break;
      case MY_DATE:      bindingUpdate(j, MYSQL_TYPE_DATE, sizeof(MYSQL_TIME)); break;
      case MY_DATE_TIME: bindingUpdate(j, MYSQL_TYPE_DATETIME, sizeof(MYSQL_TIME)); break;
      case MY_FACTOR:    bindingUpdate(j, MYSQL_TYPE_DOUBLE, 8); break;
      case MY_STR:       bindingUpdate(j, MYSQL_TYPE_STRING, 0); break;
      case MY_RAW:       bindingUpdate(j, MYSQL_TYPE_BLOB, 0); break;
      case MY_INT64:
      case MY_TIME:
        // output only
        break;
      }
    }
  }

  void bindOne(Rcpp::List params, int i = 0) {
    for (int j = 0; j < p_; ++j) {
      bool missing = false;
      Rcpp::RObject col = params[i];

      switch(types_[j]) {
      case MY_LGL:
        if (LOGICAL(col)[i] == NA_LOGICAL) {
          missing = true;
          break;
        }
        bindings_[j].buffer = &LOGICAL(col)[i];

        break;
      case MY_INT32:
        if (INTEGER(col)[i] == NA_INTEGER) {
          missing = true;
          break;
        }
        bindings_[j].buffer = &INTEGER(col)[i];
        break;
      case MY_DBL:
        if (REAL(col)[i] == NA_REAL) {
          missing = true;
          break;
        }
        bindings_[j].buffer = &REAL(col)[i];
        break;
      case MY_STR:
        if (STRING_ELT(col, i) == NA_STRING) {
          missing = true;
          break;
        } else {
          SEXP string = STRING_ELT(col, i);
          bindings_[j].buffer_length = Rf_length(string);
          bindings_[j].buffer = (void*) CHAR(string);
        }
        break;
      case MY_RAW: {
        SEXP raw = VECTOR_ELT(col, i);
        bindings_[j].buffer_length = Rf_length(raw);
        bindings_[j].buffer = RAW(raw);
      }
      case MY_FACTOR:
      case MY_DATE:
      case MY_DATE_TIME:
        Rcpp::stop("Not yet supported");
      case MY_INT64:
      case MY_TIME:
        // output only
        break;
      }
      isNull_[j] = missing;
    }
    mysql_stmt_bind_param(pStatement_, &bindings_[0]);
  }

  void bindRows(Rcpp::List params) {
    if (params.size() == 0)
      return;

    int n = Rf_length(params[0]);
    for (int i = 0; i < n; ++i)
      bindOne(params, i);
  }

  void bindingUpdate(int j, enum_field_types type, int size) {
    bindings_[j].buffer_length = size;
    bindings_[j].buffer_type = type;
    bindings_[j].is_null = &isNull_[j];
  }

};

#endif
