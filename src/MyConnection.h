#ifndef __RMYSQL_MY_CONNECTION__
#define __RMYSQL_MY_CONNECTION__

#include <Rcpp.h>
#include <mysql.h>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>

#define AS_API_STRING(X) (X.isNull() ? NULL : Rcpp::as<std::string>(X).c_str())

class MyResult;

// convenience typedef for shared_ptr to PqConnection
class MyConnection;
typedef boost::shared_ptr<MyConnection> MyConnectionPtr;

class MyConnection : boost::noncopyable {
  MYSQL* pConn_;
  MyResult* pCurrentResult_;

public:

  MyConnection(const Rcpp::Nullable<std::string>& host,
               const Rcpp::Nullable<std::string>& user,
               const Rcpp::Nullable<std::string>& password,
               const Rcpp::Nullable<std::string>& db,
               unsigned int port,
               const Rcpp::Nullable<std::string>& unix_socket,
               unsigned long client_flag,
               const Rcpp::Nullable<std::string>& groups,
               const Rcpp::Nullable<std::string>& default_file,
               const Rcpp::Nullable<std::string>& ssl_key,
               const Rcpp::Nullable<std::string>& ssl_cert,
               const Rcpp::Nullable<std::string>& ssl_ca,
               const Rcpp::Nullable<std::string>& ssl_capath,
               const Rcpp::Nullable<std::string>& ssl_cipher) :
    pCurrentResult_(NULL)
  {
    pConn_ = mysql_init(NULL);
    // Enable LOCAL INFILE for fast data ingest
    mysql_options(pConn_, MYSQL_OPT_LOCAL_INFILE, 0);
    // Default to UTF-8
    mysql_options(pConn_, MYSQL_SET_CHARSET_NAME, "UTF8");

    if (!groups.isNull())
      mysql_options(pConn_, MYSQL_READ_DEFAULT_GROUP, AS_API_STRING(groups));

    if (!default_file.isNull())
      mysql_options(pConn_, MYSQL_READ_DEFAULT_FILE,
                    AS_API_STRING(default_file));

    if (!ssl_key.isNull() || !ssl_cert.isNull() || !ssl_ca.isNull() ||
        !ssl_capath.isNull() || !ssl_cipher.isNull()) {
      mysql_ssl_set(
        pConn_,
        AS_API_STRING(ssl_key),
        AS_API_STRING(ssl_cert),
        AS_API_STRING(ssl_ca),
        AS_API_STRING(ssl_capath),
        AS_API_STRING(ssl_cipher)
      );
    }

    if (!mysql_real_connect(pConn_,
        AS_API_STRING(host),
        AS_API_STRING(user),
        AS_API_STRING(password),
        AS_API_STRING(db),
        port,
        AS_API_STRING(unix_socket),
        client_flag)) {
      mysql_close(pConn_);
      Rcpp::stop("Failed to connect: %s", mysql_error(pConn_));
    }
  }

  Rcpp::List connectionInfo() {
    return Rcpp::List::create(
      Rcpp::_["host"] = std::string(pConn_->host),
      Rcpp::_["user"] = std::string(pConn_->user),
      Rcpp::_["dbname"] = std::string(pConn_->db),
      Rcpp::_["conType"] = std::string(mysql_get_host_info(pConn_)),
      Rcpp::_["serverVersion"] = std::string(mysql_get_server_info(pConn_)),
      Rcpp::_["protocolVersion"] = (int) mysql_get_proto_info(pConn_),
      Rcpp::_["threadId"] = (int) mysql_thread_id(pConn_),
      Rcpp::_["client"] = std::string(mysql_get_client_info())
    );
  }

  std::string quoteString(std::string input) {
    // Create buffer with enough room to escape every character
    std::string output;
    output.resize(input.size() * 2 + 1);

    size_t end = mysql_real_escape_string(pConn_, &output[0],
      input.data(), input.size());
    output.resize(end);

    return output;
  }

  MYSQL* conn() {
    return pConn_;
  }

  // Cancels previous query, if needed.
  void setCurrentResult(MyResult* pResult);
  bool isCurrentResult(MyResult* pResult) {
    return pCurrentResult_ == pResult;
  }
  bool hasQuery() {
    return pCurrentResult_ != NULL;
  }

  bool exec(std::string sql) {
    setCurrentResult(NULL);

    if (mysql_real_query(pConn_, sql.data(), sql.size()) != 0)
      Rcpp::stop(mysql_error(pConn_));

    MYSQL_RES* res = mysql_store_result(pConn_);
    if (res != NULL)
      mysql_free_result(res);

    return true;
  }

  ~MyConnection() {
    try {
      mysql_close(pConn_);
    } catch(...) {};
  }

};

#endif
