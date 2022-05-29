// Copyright by Enigma

#include <logsettings.hpp>

boost::log::trivial::severity_level choose_level(
    const std::string& lev) {
  if (lev == "info")
    return boost::log::trivial::severity_level::info;
  else if (lev == "warning")
    return boost::log::trivial::severity_level::warning;
  else
    return boost::log::trivial::severity_level::error;
}

void logs(const std::string& lev) {
  boost::log::add_common_attributes();

  boost::log::core::get()->set_filter(boost::log::trivial::severity ==
                                      choose_level(lev));

  boost::log::add_console_log(
      std::clog, boost::log::keywords::format =
                     "[%TimeStamp%][%ThreadID%][%Severity%]: %Message%");

  boost::log::add_file_log(
      boost::log::keywords::file_name = "logs/Log_%N.log",
      boost::log::keywords::rotation_size = 10 * 1024 * 1024,
      boost::log::keywords::time_based_rotation =
          boost::log::sinks::file::rotation_at_time_point(0, 0, 0),
      boost::log::keywords::format = "[%TimeStamp%][%Severity%]: %Message%");
}
