# async-observer - Rails plugin for asynchronous job execution

# Copyright (C) 2007 Philotic Inc.

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


require 'open3'

module AsyncObserver; end
module AsyncObserver::Util
  def log_bracketed(name)
    begin
      RAILS_DEFAULT_LOGGER.info "#!#{name}!begin!#{Time.now.utc.xmlschema(6)}"
      yield()
    ensure
      RAILS_DEFAULT_LOGGER.info "#!#{name}!end!#{Time.now.utc.xmlschema(6)}"
    end
  end
end
