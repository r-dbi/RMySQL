#include "MyConnection.h"
#include "MyResult.h"

void MyConnection::setCurrentResult(MyResult* pResult) {
  if (pResult == pCurrentResult_)
    return;

  if (pCurrentResult_ != NULL) {
    if (pResult != NULL)
      Rcpp::warning("Cancelling previous query");

    pCurrentResult_->close();
  }
  pCurrentResult_ = pResult;
}
