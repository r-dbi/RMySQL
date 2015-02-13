#ifndef __RSQLITE_MY_UTILS__
#define __RSQLITE_MY_UTILS__

#include <Rcpp.h>
#include "MyTypes.h"

// Generic data frame utils ----------------------------------------------------

Rcpp::List inline dfResize(Rcpp::List df, int n) {
  int p = df.size();

  Rcpp::List out(p);
  for (int j = 0; j < p; ++j) {
    out[j] = Rf_lengthgets(df[j], n);
  }

  out.attr("names") = df.attr("names");
  out.attr("class") = df.attr("class");
  out.attr("row.names") = Rcpp::IntegerVector::create(NA_INTEGER, -n);

  return out;
}

Rcpp::List inline dfCreate(std::vector<MyFieldType> types, std::vector<std::string> names, int n) {
  int p = types.size();

  Rcpp::List out(p);
  out.attr("names") = names;
  out.attr("class") = "data.frame";
  out.attr("row.names") = Rcpp::IntegerVector::create(NA_INTEGER, -n);

  for (int j = 0; j < p; ++j) {
    out[j] = Rf_allocVector(typeSEXP(types[j]), n);
  }
  return out;
}

#endif
