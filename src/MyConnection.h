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

  MyConnection(std::string host, std::string user, std::string password,
               std::string db, unsigned int port, std::string unix_socket,
               unsigned long client_flag, std::string groups,
               std::string default_file) :
    pCurrentResult_(NULL)
  {

    pConn_ = mysql_init(NULL);
    // Enable LOCAL INFILE for fast data ingest
    mysql_options(pConn_, MYSQL_OPT_LOCAL_INFILE, 0);
    // Default to UTF-8
    mysql_options(pConn_, MYSQL_SET_CHARSET_NAME, "UTF8");
    if (groups != "")
      mysql_options(pConn_, MYSQL_READ_DEFAULT_GROUP, groups.c_str());
    if (default_file != "")
      mysql_options(pConn_, MYSQL_READ_DEFAULT_FILE, default_file.c_str());


    if (!mysql_real_connect(pConn_, host.c_str(), user.c_str(),
        password.c_str(), db == "" ? NULL : db.c_str(), port,
        unix_socket == "" ? NULL : unix_socket.c_str(), client_flag)) {
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
