#ifndef __RMYSQL_MY_BINDING__
#define __RMYSQL_MY_BINDING__

#include <Rcpp.h>
#include <mysql.h>
#include "MyTypes.h"
#include <ctime>

class MyBinding {
  MYSQL_BIND binding_;
  std::vector<unsigned char> buffer_;

  MyFieldType type_;
  unsigned long length_;
  my_bool is_null_, error_;

public:
  MyBinding(MyFieldType type): type_(type) {
    switch(type) {
    case MY_INT32:
      binding_.buffer_type = MYSQL_TYPE_LONG;
      buffer_.resize(4);
      break;
    case MY_DBL:
      binding_.buffer_type = MYSQL_TYPE_DOUBLE;
      buffer_.resize(8);
      break;
    case MY_DATE:
      binding_.buffer_type = MYSQL_TYPE_DATE;
      buffer_.resize(sizeof(MYSQL_TIME));
      break;
    case MY_DATE_TIME:
      binding_.buffer_type = MYSQL_TYPE_TIME;
      buffer_.resize(sizeof(MYSQL_TIME));
      break;
    case MY_TIME:
      binding_.buffer_type = MYSQL_TYPE_DATETIME;
      buffer_.resize(sizeof(MYSQL_TIME));
      break;
    case MY_INT64:
    case MY_STR:
    case MY_RAW:
      binding_.buffer_type = MYSQL_TYPE_STRING;
      // buffers might be arbitrary length, so leave size and use
      // alternative strategy 0
      break;
    }

    binding_.buffer = &buffer_[0];
    binding_.buffer_length = buffer_.size();
    binding_.length = &length_;
    binding_.is_null = &is_null_;
    binding_.is_unsigned = true;
    binding_.error = &error_;
  }

  void retry(MYSQL_STMT* pStatement, int j) {
    if (length_ > 0) {
      buffer_.resize(length_);
      binding_.buffer_length = length_;
      mysql_stmt_fetch_column(pStatement, &binding_, 0, j);

      // Reset buffer length to zero for next row
      binding_.buffer_length = 0;
    }
  }

  // Value accessors -----------------------------------------------------------
  int valueInt() {
    return is_null_ ? NA_INTEGER : *((int*) &buffer_[0]);
  }

  int valueDouble() {
    return is_null_ ? NA_REAL : *((double*) &buffer_[0]);
  }

  SEXP valueString() {
    if (is_null_)
      return NA_STRING;

    buffer_.push_back('\0');  // ensure string is null terminated
    char* val = (char*) &buffer_[0];

    return Rf_mkCharCE(val, CE_UTF8);
  }

  SEXP valueRaw() {
    SEXP bytes = Rf_allocVector(RAWSXP, length_);
    memcpy(RAW(bytes), &buffer_[0], length_);

    return bytes;
  }

  int valueDateTime() {
    if (is_null_)
      return NA_INTEGER;

    MYSQL_TIME* mytime = (MYSQL_TIME*) &buffer_[0];

    struct tm t = { 0 };
    t.tm_year = mytime->year;
    t.tm_mon = mytime->month;
    t.tm_mday = mytime->day;
    t.tm_hour = mytime->hour;
    t.tm_min = mytime->minute;
    t.tm_sec = mytime->second;

    return mktime(&t);
  }

  int valueTime() {
    if (is_null_)
      return NA_INTEGER;

    MYSQL_TIME* mytime = (MYSQL_TIME*) &buffer_[0];
    return mytime->hour * 3600 + mytime->minute * 60 + mytime->second;
  }

  void setListValue(SEXP x, int i) {
    switch(type_) {
    case MY_INT32:
      INTEGER(x)[i] = valueInt();
      break;
    case MY_DBL:
      REAL(x)[i] = valueDouble();
      break;
    case MY_INT64:
    case MY_STR:
      SET_STRING_ELT(x, i, valueString());
      break;
    case MY_DATE:
    case MY_DATE_TIME:
      INTEGER(x)[i] = valueDateTime();
      break;
    case MY_TIME:
      INTEGER(x)[i] = valueTime();
      break;
    case MY_RAW:
      SET_VECTOR_ELT(x, i, valueRaw());
      break;
    }
  }

};

#endif
