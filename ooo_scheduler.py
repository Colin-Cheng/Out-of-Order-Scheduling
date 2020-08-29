import logging

# Import functions data structures used by Out of Order Scheduler.
from helpers import *


# Main scheduler class.
class out_of_order_scheduler:


    def __init__ (self, infilename, outfilename):

        # Constant for this project.
        ARCH_REGS_COUNT = 32

        # Parse input file. Open output file.
        self.input = self.parse_input_file(infilename)
        (self.num_phy_regs, self.issue_width) = next(self.input)
        self.out_file = open(outfilename, "w")

        # Various queues or latches connecting different pipeline stages.
        self.decode_queue = pipeline_stage(self.issue_width)
        self.rename_queue = pipeline_stage(self.issue_width)
        self.dispatch_queue = pipeline_stage(self.issue_width)
        self.issue_queue = []
        self.reorder_buffer = []
        self.lsq = load_store_queue()
        self.executing_queue = []
        self.ready_to_free = []

        # Structures to track registers.
        self.map_table = reg_map(ARCH_REGS_COUNT)
        self.free_list = free_list(self.num_phy_regs)
        self.ready_table = ready_queue(self.num_phy_regs)
        self.freeing_registers = []

        # Initially map R0->P0, R1->P1 and so on.
        for register in range(ARCH_REGS_COUNT):
            self.map_table.put(register, self.free_list.get_free_reg())

        # Instructions under consideraion so far.
        self.instructions = []

        # Start from cycle 0.
        self.cycle = 0

        # Track if we are currently fetching an instruction.
        # Used to detect when we have finished scheduling all instructions.
        self.fetching = True

        # Did any stage in the pipeline progress in last cycle?
        # Used to detect if pipeline is stuck because of bad scheduler design.
        self.has_progressed = True


    #
    # Main scheduler functions
    #
    #
    def schedule (self):

        self.fetching = True
        self.has_progressed = True

        while self.is_scheduling() and self.has_progressed:
            
            logging.info("Scheduling: %s" % self)
            
            self.has_progressed = False

            # We process pipeline stages in opposite order to (try to) clear up
            # the subsequent stage before sending instruction forward from any
            # given stage.
            self.free_reg()
            self.commit()
            self.writeback()
            self.issue()
            self.dispatch()
            self.rename()
            self.decode()
            self.fetch()

            # Move on to the next cycle.
            self.advance_cycle()


    def advance_cycle (self):
        for free_reg in self.freeing_registers:
            self.free_list.free(free_reg)
        self.freeing_registers = []

        self.cycle += 1

        logging.debug("Advanced scheduler to cycle # %d" % self.cycle)


    def made_progress (self):
        self.has_progressed = True


    def is_scheduling (self):
        return (
            self.fetching
            or any(not inst.has_commited() for inst in self.instructions)
        )


    #
    # Pipeline stages start here
    # #################################
    #


    #
    # Fetch Stage
    #
    def fetch_inst (self):
        try:
            return next(self.input)
        except StopIteration:
            self.fetching = False
            return None

    def fetch (self):
        fetched = 0
        while self.fetching and fetched < self.issue_width:
            inst = self.fetch_inst()
            if inst is not None:
                inst.fetch_cycle = self.cycle
                self.instructions.append(inst)
                self.decode_queue.pushQ(inst)

                fetched += 1

                self.made_progress()
                logging.debug("Fetched: %s" % inst)


    #
    # Decode Stage
    #
    def decode (self):
        while not self.decode_queue.is_empty():
            inst = self.decode_queue.popQ()
            inst.decode_cycle = self.cycle
            self.rename_queue.pushQ(inst)

            self.made_progress()
            logging.debug("Decoded: %s" % inst)


    #
    # Rename Stage
    #
    def rename (self):

        while not self.rename_queue.is_empty():
            inst = self.rename_queue.popQ()

            #if there are free registers
            if self.free_list.is_free():

                #rename source registers
                inst.src_reg_0 = self.map_table.get(inst.src_reg_0)
                if inst.src_reg_1 != None:
                    inst.src_reg_1 = self.map_table.get(inst.src_reg_1)


                #rename destination register and add it to map table
                if inst.dst_reg!= None: 

                    #write the overwritten register
                    last_mapping = self.map_table.get(inst.dst_reg)     
                    inst.overwritten = last_mapping

                    pdr = self.free_list.get_free_reg()
                    self.map_table.put(inst.dst_reg,pdr)
                    inst.dst_reg = pdr

                #store cycle number for rename stage
                inst.rename_cycle = self.cycle  
                #update inst.renamed
                inst.renamed = True
                
                #push the renamed instructions into dispatch queue
                self.dispatch_queue.pushQ(inst)

                self.made_progress()
                logging.debug("Renamed: %s" % inst)

            #if it is a store instruction and does not have destination register
            elif inst.dst_reg == None: 

                #rename source registers
                inst.src_reg_0 = self.map_table.get(inst.src_reg_0)
                if inst.src_reg_1 != None:
                    inst.src_reg_1 = self.map_table.get(inst.src_reg_1)

                #store cycle number for rename stage
                inst.rename_cycle = self.cycle  
                #update inst.renamed
                inst.renamed = True
                
                #push the renamed instructions into dispatch queue
                self.dispatch_queue.pushQ(inst)

                self.made_progress()
                logging.debug("Renamed: %s" % inst)

            #if no free registers available
            else:
                self.rename_queue.insertQ(inst)
                break

    #
    # Dispatch Stage
    #
    def dispatch (self):
        while not self.dispatch_queue.is_empty():
            inst = self.dispatch_queue.popQ()

            #push all instrucitons into issue queue and reorder buffer
            self.issue_queue.append(inst)
            self.reorder_buffer.append(inst)

            #push load/store instructions into load_store_queue
            if inst.is_load_store_inst():
                self.lsq.append(inst)

            #change the destination register to not ready
            if (inst.dst_reg != None):
                self.ready_table.clear(inst.dst_reg)

            #store cycle number for dispatch stage
            inst.dispatch_cycle = self.cycle

            self.made_progress()
            logging.debug("Dispatched: %s" % inst)


    #
    # Issue Stage
    #
    def issue (self):

        issued = 0
        for inst in list(self.issue_queue[:]):
 
            #
            # TODO: Your code here
            #
            # TODO: Do not forget to include following where appropriate:
            # self.made_progress()
            # logging.debug("Issued: %s" % inst)
            #

            #if the number of current executing instruction is equal to the width
            #no more instruction can be issued
            if len(self.executing_queue) == self.issue_width:
                break

            #if instruction is ready, issue the instruction
            if self.is_inst_ready(inst):

                #change the destination register to not ready
                if inst.dst_reg != None:
                    self.ready_table.clear(inst.dst_reg)

                #store the cycle number for issue stage
                inst.issue_cycle = self.cycle

                #add the instruction to executing_queue
                self.executing_queue.append(inst)

                #remove the instruction from issue queue
                self.issue_queue.remove(inst)

                logging.debug("Issued: %s" % inst)
                issued += 1

        #if number of issued instruction > 0, progress has been made
        if issued > 0:
            self.made_progress()





    #
    # Writeback Stage
    #
    def writeback (self):

        #
        # TODO: Your code here
        #
        # TODO: Do not forget to include following where appropriate:
        # self.made_progress()
        # logging.debug("Writeback: %s" % inst)
        #
        # TODO: Also:
        # self.made_progress()
        # logging.debug("Writeback Load/Store: %s" % inst)
        #
        mem_access_num = 0
        for inst in self.executing_queue[:]:
            #if the instruction is not load/store
            if not inst.is_load_store_inst():

                #change destination register to ready
                if inst.dst_reg != None:
                    self.ready_table.ready(inst.dst_reg)

                #remove it from executing_queue
                self.executing_queue.remove(inst)

                #store cycle number for writeback stage
                inst.writeback_cycle = self.cycle

                self.made_progress()
                logging.debug("Writeback: %s" % inst)

            #if the instruction is load/store
            else:

                if mem_access_num == self.issue_width:
                    continue

                #if instruction can be executed
                if self.lsq.can_execute(inst):

                    #change destination register to ready
                    if inst.dst_reg != None:
                        self.ready_table.ready(inst.dst_reg)

                    #remove it from load_store_queue and executing_queue
                    self.lsq.remove(inst)
                    self.executing_queue.remove(inst)

                    #store cycle number for writeback stage
                    inst.writeback_cycle = self.cycle

                    mem_access_num += 1

                    self.made_progress()
                    logging.debug("Writeback Load/Store: %s" % inst)





    #
    # Commit Stage
    #
    def commit (self):

        #
        # TODO: Your code here
        #
        # TODO: Do not forget to include following where appropriate:
        # self.made_progress()
        # logging.debug("Committed: %s" % inst)
        #
        for inst in self.reorder_buffer[:]:

            if inst.has_writtenback():
                #store cycle number for commit stage
                inst.commit_cycle = self.cycle

                #remove instruction from reorder buffer
                self.reorder_buffer.remove(inst)

                #add instruction into ready_to_free list
                self.ready_to_free.append(inst)

                self.made_progress()
                logging.debug("Committed: %s" % inst)

            else:
                break

    #free overwritten registers one cycle after instruction committed
    def free_reg (self):
        for inst in self.ready_to_free[:]:

            #free the overwritten register
            self.free_list.free(inst.overwritten)

            #remove register from ready_to_free list
            self.ready_to_free.remove(inst)



    def is_inst_ready (self, inst):

        if (not self.ready_table.is_ready(inst.src_reg_0)):
            return False
        
        if (inst.src_reg_1 is not None) and (not self.ready_table.is_ready(inst.src_reg_1)):
            return False

        if inst.is_load_store_inst():
            return self.lsq.can_execute(inst)

        return True




    #
    # File I/O functions
    # #################################
    #

    # Parse input file.
    def parse_input_file (self, infilename):

        # Constant for this project.
        PHY_REG_COUNT_MIN = 32

        try:
            with open(infilename, 'r') as file:
                
                # Regex strings to read field out of file line strings.
                config_parser = re.compile("^(\\d+),(\\d+)$")
                inst_parser = re.compile("^([RILS]),(\\d+),(\\d+),(\\d+)$")

                # Try to parse header
                header = file.readline()
                configs = config_parser.match(header)
                if configs:
                    (num_phy_reg, issue_width) = configs.group(1, 2)
                    num_phy_reg = int(num_phy_reg)
                    issue_width = int(issue_width)

                    if num_phy_reg < PHY_REG_COUNT_MIN:
                        print("Error: Invalid input file header: Number of "
                                "physical register is less than allowed minimum of %d" % (PHY_REG_COUNT_MIN))
                        sys.exit(1)

                    yield (num_phy_reg, issue_width)
                else:
                    print("Error: Invalid input file header!")
                    sys.exit(1)

                # Parse all remaining lines one by one.
                for (index, line) in enumerate(file):
                    configs = inst_parser.match(line)
                    if configs:
                        (Insts, op0, op1, op2) = configs.group(1, 2, 3, 4)

                        yield instruction(index, Insts, int(op0), int(op1), int(op2))
                    else:
                        print("Error: Invalid inst_set: %s" % (line))
                        sys.exit(1)

        except IOError:
            print("Error parsing input file!")
            sys.exit(1)


    def generate_output_file (self):
        if self.is_scheduling():
            self.out_file.write("")
            self.out_file.close()
            return

        for inst in self.instructions:
            self.out_file.write("%s,%s,%s,%s,%s,%s,%s\n" % (
                inst.fetch_cycle,
                inst.decode_cycle,
                inst.rename_cycle,
                inst.dispatch_cycle,
                inst.issue_cycle,
                inst.writeback_cycle,
                inst.commit_cycle,
            ))

        self.out_file.close()

    def __str__ (self):
        return "[out_of_order_scheduler cycle=%d]" % (self.cycle)
        
