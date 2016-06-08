#ifndef __RMYSQL_MY_BINDING__
#define __RMYSQL_MY_BINDING__

#include <Rcpp.h>
#include <mysql.h>
#include "MyTypes.h"
#include <ctime>

class MyBinding {
  MYSQL_STMT* pStatement_;

  int p_;
  std::vector<MYSQL_BIND> bindings_;
  std::vector<my_bool> isNull_;
  std::vector<MyFieldType> types_;
  std::vector<MYSQL_TIME> timeBuffers_;

public:

  void setUp(MYSQL_STMT* pStatement) {
    pStatement_ = pStatement;
    p_ = mysql_stmt_param_count(pStatement_);

    bindings_.resize(p_);
    types_.resize(p_);
    isNull_.resize(p_);
    timeBuffers_.resize(p_);
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
      case MY_INT32:     bindingUpdate(j, MYSQL_TYPE_LONG, 4); break;
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

  void bindRow(Rcpp::List params, int i) {
    for (int j = 0; j < p_; ++j) {
      bool missing = false;
      Rcpp::RObject col = params[j];

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
        if (ISNA(REAL(col)[i])) {
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
      case MY_DATE:
      case MY_DATE_TIME:
        if (ISNAN(REAL(col)[i])) {
          missing = true;
          break;
        } else {
          double val = REAL(col)[i];
          setTimeBuffer(j, val * (types_[j] == MY_DATE ? 86400 : 1));
          bindings_[j].buffer_length = sizeof(MYSQL_TIME);
          bindings_[j].buffer = &timeBuffers_[j];
        }
        break;
      case MY_FACTOR:
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

  void bindingUpdate(int j, enum_field_types type, int size) {
    bindings_[j].buffer_length = size;
    bindings_[j].buffer_type = type;
    bindings_[j].is_null = &isNull_[j];
  }

  void setTimeBuffer(int j, time_t time) {
    struct tm* tm = gmtime(&time);

    timeBuffers_[j].year = tm->tm_year + 1900;
    timeBuffers_[j].month = tm->tm_mon + 1 ;
    timeBuffers_[j].day = tm->tm_mday;
    timeBuffers_[j].hour = tm->tm_hour;
    timeBuffers_[j].minute = tm->tm_min;
    timeBuffers_[j].second = tm->tm_sec;
  }

};

#endif
