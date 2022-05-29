//Copyright 2022 by Winter Solider

#include <iostream>
#include <boost/program_options.hpp>
#include <boost/unordered_map.hpp>
#include <rocksdb/slice.h>

#include <database.hpp>
#include <logsettings.hpp>

namespace po = boost::program_options;

int main(int argc, char **argv){
  po::options_description desc("Options");
  desc.add_options()
      ("help", "help information")
      ("log_level", po::value<std::string>(), "level logging")
      ("thread_count", po::value<unsigned>(), "count of threads")
      ("output", po::value<std::string>(), "path result");
  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);
  }
  catch (po::error &e) {
    std::cout << e.what() << std::endl;
    std::cout << desc << std::endl;
    return 1;
  }
  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }
  if (!vm.count("log_level")
      || !vm.count("thread_count")
      || !vm.count("output")) {
    std::cout << "error: bad format" << std::endl << desc << std::endl;
    return 1;
  }
  std::string log_lev = vm["log_level"].as<std::string>();
  std::size_t threads = vm["thread_count"].as<unsigned>();
  std::string outp_dir = vm["output"].as<std::string>();

  logs(log_lev);

  std::string inp_dir = "inpBD";
  make_inp_BD(inp_dir);

  My_BD manager(inp_dir, outp_dir, threads);
  manager.start_process();
  return 0;
}

