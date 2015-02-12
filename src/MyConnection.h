#ifndef __RMYSQL_MY_CONNECTION__
#define __RMYSQL_MY_CONNECTION__

#include <Rcpp.h>
#include <mysql.h>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>

// convenience typedef for shared_ptr to PqConnection
class MyConnection;
typedef boost::shared_ptr<MyConnection> MyConnectionPtr;

class MyConnection : boost::noncopyable {
  MYSQL* pConn_;

public:

  MyConnection(std::string host, std::string user, std::string password,
               std::string db, unsigned int port, std::string unix_socket,
               unsigned long client_flag, std::string groups,
               std::string default_file) {

    pConn_ = mysql_init(NULL);
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
      Rcpp::_["client"] = mysql_get_server_info(pConn_)
    );
  }

  std::string quoteString(std::string input) {
    // Create buffer with enough room to escape every character
    std::string output(' ', input.length() * 2 + 1);

    mysql_real_escape_string(pConn_, &output[0], input.c_str(), input.size());

    // Find null
    size_t end = output.find_first_of('\0');
    if (end != std::string::npos)
      output.resize(end);

    return output;
  }

  ~MyConnection() {
    try {
      mysql_close(pConn_);
    } catch(...) {};
  }

};

#endif
