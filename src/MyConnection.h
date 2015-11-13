#ifndef __RMYSQL_MY_CONNECTION__
#define __RMYSQL_MY_CONNECTION__

#include <Rcpp.h>
#include <mysql.h>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>

class MyResult;

// convenience typedef for shared_ptr to PqConnection
class MyConnection;
typedef boost::shared_ptr<MyConnection> MyConnectionPtr;

class MyConnection : boost::noncopyable {
  MYSQL* pConn_;
  MyResult* pCurrentResult_;

public:

  MyConnection(Rcpp::Nullable < std::string > host,
               Rcpp::Nullable < std::string > user,
               Rcpp::Nullable < std::string > password,
               Rcpp::Nullable < std::string > db,
               unsigned int port,
               Rcpp::Nullable < std::string > unix_socket,
               unsigned long client_flag,
               Rcpp::Nullable < std::string > groups,
               Rcpp::Nullable < std::string > default_file) :
    pCurrentResult_(NULL)
  {

    pConn_ = mysql_init(NULL);
    // Enable LOCAL INFILE for fast data ingest
    mysql_options(pConn_, MYSQL_OPT_LOCAL_INFILE, 0);
    // Default to UTF-8
    mysql_options(pConn_, MYSQL_SET_CHARSET_NAME, "UTF8");
    if (!groups.isNull())
      mysql_options(pConn_, MYSQL_READ_DEFAULT_GROUP,
                    Rcpp::as<std::string>(groups).c_str());
    if (!default_file.isNull())
      mysql_options(pConn_, MYSQL_READ_DEFAULT_FILE,
                    Rcpp::as<std::string>(default_file).c_str());

    if (!mysql_real_connect(pConn_,
        host.isNull() ? NULL : Rcpp::as<std::string>(host).c_str(),
        user.isNull() ? NULL : Rcpp::as<std::string>(user).c_str(),
        password.isNull() ? NULL : Rcpp::as<std::string>(password).c_str(),
        db.isNull() ? NULL : Rcpp::as<std::string>(db).c_str(),
        port,
        unix_socket.isNull() ? NULL : Rcpp::as<std::string>(unix_socket).c_str(),
        client_flag)) {
      mysql_close(pConn_);
      Rcpp::stop("Failed to connect: %s", mysql_error(pConn_));
    }
  }

  Rcpp::List connectionInfo() {
    return Rcpp::List::create(
      Rcpp::_["host"] = mysql_get_host_info(pConn_),
      Rcpp::_["server"] = mysql_get_server_info(pConn_),
      Rcpp::_["client"] = mysql_get_client_info()
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
