#!/usr/env/ruby
# daisy.rb

require 'rio'
require 'drb'
require 'fileutils'
require 'rinda/ring'
require 'rinda/tuplespace'

module Daisy
	APP_ROOT = ENV["RAILS_ROOT"] || File.dirname(__FILE__)
	
	module Commands
		FLOWER_COMMANDS = [
			:add_petal,
			:remove_petal,
			:stop_petals,
			:start_petals,
			:test_petals,
			:reset_petals
		]
		
		GENERAL_COMMANDS = [
			:ping,
			:source
		]
	end
	
	module Locatable
		def host
			drb_init
			u = URI.parse(DRb.uri)
			"#{u.host}:#{u.port}"
		end
		
		def ip
			raise NotImplementedError
		end
		
		def drb_init
			begin
				DRb.current_server
			rescue
				DRb.start_service
			end
		end
		
	end # Locatable
	
	module Server 
			include Locatable
			
			def start
				raise "Already running" if is_running?
				pid = Process.fork 
				if pid.nil? #child
					self.run
				end
				#parent
				Process.detach(pid)
				@rio < pid
			end
			
			def stop
				raise "Not running" unless is_running?
				@rio > pid
				Process.kill(9, pid)
				DRb.stop_service
				@rio.delete
			end
			
			def run
				drb_init

				#TODO - associate with ringserver

				DRb.thread.join
			end
			
			#protected
			def is_running?
				@rio = rio("#{APP_ROOT}/data/pids/server.pid")
				@rio.exist?
			end
			
	end #Server
	
	class Flower
		include Server
		include DRbUndumped
		include Commands
		
		def initialize()
			self.petals = []
			
			# all commands
			cmd_obs = Gardener.garden.commands.notify 'write', [:command, nil, nil]
			
			Thread.start(self, Gardener.garden) do |flower, garden|
				#garden.flower(flower.host)
				cmd_obs.each do |cmd|
					
					#check to see if this command is specific to this flower, take it off the stack if so
					# if it's specific to a flower, but not this, ignore
					unless cmd[1][1].nil?
						if cmd[1][1]==self.host
							garden.commands.take(cmd[1])
						else
							next
						end
					end
					
					#check to see if this is a valid flower command
					if (GENERAL_COMMANDS | FLOWER_COMMANDS).include? cmd[1][2][0]
						#if so, respond to it
						
						#this means take it off the queue, and do something with the data
						
						garden.status self.host, [ :begin, [ cmd[1][2] ] ]
						begin
							output = flower.handle garden,cmd[1][2]
							garden.status self.host, [ :end, [ cmd[1][2] ], output ]
						rescue Exception
							garden.status self.host, [ :error, [ cmd[1][2] ], $!.to_str ]
						end
						
					end
					
				end
			end
			
			run
		end
		
		#this handler enforces a naming convention on handler methods,
		# they must be of the form handle_#{command}, and can take the passed tuple as an argument
		#   tuple arg is of the form [:the_command, [args]]
		def handle(garden,obj)
			puts ":handle called with #{obj.inspect}"
			begin
				#puts ":handle calling :handle_#{obj[0].to_s}"
				output = self.method("handle_#{obj[0].to_s}".to_sym).call(obj[1])
			rescue
				puts "could not fire :handle_#{obj[0]}"
				raise $!
			end
			puts ":handle finished"
			output
		end
		
		def as_tuple
			[:flower, self.host]
		end
		
		def handle_ping(args)
			#ping simply pings the server with the current flower struct
			Gardener.garden.flower(self.host, self.petals.map{|petal| petal.worker.class.to_s})
			nil
		end
		
		#This method responds to the command [:source, [module_name, path]]
		# it will add a sourcefile to the local runtime
		# module_name must be a string
		# path must be an absolute filepath, most likely to a place on the network
		def handle_source(args)
			self.add_petal_source(args[0], args[1])
			nil
		end
		
		#This method responds to the command [:add_petal, [module_name, klass_name]]
		# klass_name must be of the form Module::*::Class
		def handle_add_petal(args)
			puts ":handle_add_petal adding petal type #{args.join("::")} to flower at #{self.host}"
			klass = Daisy::Flower.const_get(args[0]).const_get(args[1])
			add_petal(Petal.new(klass.new))
			nil
		end
		
		#This method responds to the command [:remove_petal, [klass, id=nil]]
		def handle_remove_petal(args)
			handle_stop_petal(args)
			puts ":handle_remove_petal removing petal type #{args[0]}"
			targets = [] | self.petals.select{|petal| petal.class==args[0] }
			remove_petals targets
			nil
		end
		
		#This method responds to the command [:start_petal, [klass, id=nil]]
		def handle_start_petal(args)
			puts ":handle_start_petal firing for type #{args[0]}"
			targets = [] | self.petals.select{|petal| petal.class==args[0]}
			warning "TODO - multithreading"
			start_petals targets
			nil
		end
		
		#This method responds to the command [:stop_petal, [klass, id=nil]]
		def handle_stop_petal(args)
			warn "Called unavailable method Flower#handle_stop_petal(args)"
			return nil
			puts ":handle_stop_petal firing for type #{args[0]}"
			targets.each do |target| 
				target.stop unless target.ready?;
			end
			nil
		end
		
		def handle_test_petals(args)
			puts ":handle_test_petals firing"
			test_petals self.petals
		end
		
		def handle_reset_petals(args)
			puts ":handle_reset firing"
			reset_petals args
		end
		
		attr_accessor :petals
		
		
		def add_petal_source(module_name, filepath)
			autoload(module_name,filepath)
		end
		
		def has_source?(name)
			!autoload?(name).nil?
		end

		protected
		
		
		def add_petal(petal)
			[:work, :queue, :ready?, :test].each do |sym|
				raise "Petal is missing #{sym.to_s}" unless petal.worker.respond_to? sym
			end
			self.petals << petal
		end
		
		def remove_petals(*petals)
			puts "removing petals #{petals.inspect}"
			self.petals = self.petals - petals
		end
		
		def start_petals(*petals)
			puts "starting petals #{petals.inspect}"
			petals.each{|petal| petal.work}
		end
		
		def test_petals(*petals)
			puts "testing petals #{petals.inspect}"
			petals.map{|petal| petal[0].test_worker}.join("\r\n")
		end
		
		def reset_petals(*args)
			puts "resetting all petals"
			self.petals.each{|petal| petal.stop}
			self.petals = []
		end
		
	end # Flower
	
	class Petal
		include DRbUndumped
		
		def initialize(worker)
			self.worker = worker
		end
		
		def description
			nil
		end
		
		def as_tuple
			[:petal]
		end
		
		def work
			self.worker.work
		end
		
		def stop
			#TODO
		end
		
		def test_worker
			self.worker.test
		end
		
		attr_accessor :worker
	end # Petal
	
	class Gardener < Rinda::RingServer
		include Server
		
		def initialize
			g = Garden.new
			drb_init
			super(g)
			run
		end

		class << self
			def garden
				begin
					DRb.current_server
				rescue DRb::DRbServerNotFound
					DRb.start_service
				end
				Rinda::RingFinger.primary
			end
		end
		
	end # Gardener

	class Garden < Rinda::TupleSpace
		include Locatable
		
		def initialize
			super()

			[:statuses, :commands, :flowers, :petals].each do |ns|
				ts = Rinda::TupleSpace.new()
				ts.class.send :define_method, :all do
					read_all([nil, nil]) | read_all([nil, nil, nil])
				end
				
				self.class.send :define_method, ns do
					read([ns.to_sym, nil])[1]
				end
				
				if ns==:flowers
					def ts.flower(host)
						self.read [:flower, host, nil]
					end
				end
				
				self.write [ns, ts]
			end
			
		end
		
		#Example:
		# command( :add_petal, flower.host, Twice::Downloader )
		def command(command, host=nil, *args)
			cmd = as_tuple(:command, host, [command, args.flatten])
			self.commands.write cmd
			puts "Sent command #{cmd.inspect}"
			
			if command==:source
				autoload(cmd.last.last.first, cmd.last.last[1])
				puts "Added autoload #{cmd.last.last.first}"
			end
		end
		
		#Example:
		# status "druby://INT-TECH-07.rpa.com:1234", [:begin, [ :ping, [] ] ]
		def status(host, *args)
			#remove old statuses for this host
			self.statuses.read_all([:status, host, nil]).each do |stat|
				self.statuses.take stat
			end
			stat = as_tuple(:status, host, args)
			self.statuses.write stat
			puts "Received status #{stat.inspect}"
		end
		
		#here, *args is a list of the petals current on this flower
		def flower(host, *args)
			begin
				t = self.flowers.take([:flower, host, nil], 5)
			rescue
			end
			flw = as_tuple(:flower, host, args)
			self.flowers.write flw
			puts "Received flower #{flw.inspect}"
		end
		
		def as_tuple(*args)
			args
		end
	end # Garden
	
end # Daisy



















