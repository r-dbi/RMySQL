#ifndef __RMYSQL_MY_TYPES__
#define __RMYSQL_MY_TYPES__

enum MyFieldType {
  MY_INT32,
  MY_INT64,   // output only
  MY_DBL,
  MY_STR,
  MY_DATE,
  MY_DATE_TIME,
  MY_TIME,   // output only
  MY_RAW,
  MY_FACTOR, // input only
  MY_LGL     // input only
};

// http://dev.mysql.com/doc/refman/5.7/en/c-api-data-structures.html
inline MyFieldType variableType(enum_field_types type, bool binary) {
  switch(type) {
  case MYSQL_TYPE_TINY:
  case MYSQL_TYPE_SHORT:
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_YEAR:
    return MY_INT32;

  case MYSQL_TYPE_LONGLONG:
    return MY_INT64;

  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_NEWDECIMAL:
  case MYSQL_TYPE_FLOAT:
  case MYSQL_TYPE_DOUBLE:
    return MY_DBL;
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_NEWDATE:
    return MY_DATE_TIME;
  case MYSQL_TYPE_DATE:
    return MY_DATE;
  case MYSQL_TYPE_TIME:
    return MY_TIME;
  case MYSQL_TYPE_BIT:
  case MYSQL_TYPE_ENUM:
  case MYSQL_TYPE_STRING:
  case MYSQL_TYPE_VAR_STRING:
  case MYSQL_TYPE_VARCHAR:
    return binary ? MY_RAW : MY_STR;
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
    return binary ? MY_RAW : MY_STR;
  case MYSQL_TYPE_SET:
    return MY_STR;
  case MYSQL_TYPE_GEOMETRY:
    return MY_RAW;
  case MYSQL_TYPE_NULL:
    return MY_INT32;
  default:
    throw std::runtime_error("Unimplemented MAX_NO_FIELD_TYPES");
  }
}

inline std::string typeName(MyFieldType type) {
  switch(type) {
  case MY_INT32:       return "integer";
  case MY_INT64:       return "integer64";
  case MY_DBL:         return "double";
  case MY_STR:         return "string";
  case MY_DATE:        return "Date";
  case MY_DATE_TIME:   return "POSIXct";
  case MY_TIME:        return "time";
  case MY_RAW:         return "raw";
  case MY_FACTOR:      return "factor";
  case MY_LGL:         return "logical";
  }
}

inline SEXPTYPE typeSEXP(MyFieldType type) {
  switch(type) {
  case MY_INT32:       return INTSXP;
  case MY_INT64:       return INTSXP;
  case MY_DBL:         return REALSXP;
  case MY_STR:         return STRSXP;
  case MY_DATE:        return INTSXP;
  case MY_DATE_TIME:   return REALSXP;
  case MY_TIME:        return INTSXP;
  case MY_RAW:         return VECSXP;
  case MY_FACTOR:      return INTSXP;
  case MY_LGL:         return LGLSXP;
  }
}

std::string inline rClass(Rcpp::RObject x) {
  Rcpp::RObject klass_ = x.attr("class");
  std::string klass;
  if (klass_ == R_NilValue)
    return "";

  Rcpp::CharacterVector klassv = Rcpp::as<Rcpp::CharacterVector>(klass_);
  return std::string(klassv[0]);
}

inline MyFieldType variableType(Rcpp::RObject type) {
  std::string klass = rClass(type);

  switch (TYPEOF(type)) {
  case LGLSXP:
    return MY_LGL;
  case INTSXP:
    if (klass == "factor")  return MY_FACTOR;
    return MY_INT32;
  case REALSXP:
    if (klass == "Date")    return MY_DATE;
    if (klass == "POSIXct") return MY_DATE_TIME;
    return MY_DBL;
  case STRSXP:
    return MY_STR;
  }

  Rcpp::stop("Unsupported column type %s", Rf_type2char(TYPEOF(type)));
  return MY_STR;
}


#endif
