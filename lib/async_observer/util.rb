
require 'open3'

module AsyncObserver; end
module AsyncObserver::Util
  def plumb(outio, inios)
    loop do
      IO.select(inios)[0].each do |inio|
        data = inio.read()
        if data.nil? or data == ''
          inios -= [inio] # EOF
        else
          outio.write(data)
        end
      end
      break if inios.empty?
    end
  end

  def plopen(cmd, io)
    Open3.popen3(cmd) do |pin,pout,perr|
      yield(pin)
      pin.close()
      plumb(io, [pout, perr])
    end
  end

  def log_bracketed(name)
    begin
      RAILS_DEFAULT_LOGGER.info "#!#{name}!begin!#{Time.now.utc.xmlschema(6)}"
      yield()
    ensure
      RAILS_DEFAULT_LOGGER.info "#!#{name}!end!#{Time.now.utc.xmlschema(6)}"
    end
  end
end
