// Copyright by Enigma

#ifndef INCLUDE_LOGSETTINGS_HPP_
#define INCLUDE_LOGSETTINGS_HPP_

#include <string>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/file.hpp>

boost::log::trivial::severity_level choose_level(
    const std::string& lev);
void logs(const std::string& lev);

#endif  // INCLUDE_LOGSETTINGS_HPP_
