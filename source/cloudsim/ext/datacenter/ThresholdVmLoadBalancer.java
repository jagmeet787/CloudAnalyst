package cloudsim.ext.datacenter;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import cloudsim.ext.Constants;
import cloudsim.ext.event.CloudSimEvent;
import cloudsim.ext.event.CloudSimEventListener;
import cloudsim.ext.event.CloudSimEvents;


/**
 * This class implements {@link VmLoadBalancer} as a Threshold load balancer. It consist of 
 * tUnder and tUpper constant values so it can differentiate under loaded and overloaded nodes.
 * Processes  are assigned immediately upon creation to hosts. Initially, all the processors are 
 * considered to be under loaded. If node is under loaded then process allocated locally.
 * Otherwise, a remote under loaded processor is selected, and if no such host exists, 
 * the process is also allocated locally. A node is Under loaded: if(load < tUnder), Medium
 * loaded if(tUnder: ≤ load ≤ tUpper) and Overloaded: if(load > tUpper)
 * The Threshold Algorithm implements CloudSimEventListener to get notified of the VM's
 * being allocated and freed up by the {@link DatacenterController}
 * @author Jagmeet Singh
 * Reference: P. L. McEntire, J. G. O'Reilly, and R. E. Larson, Distributed Computing:
 * Concepts and Implementations. New York: IEEE Press, 1984.
 */

public class ThresholdVmLoadBalancer extends VmLoadBalancer implements CloudSimEventListener {
	
	/** Holds the count current active allocations on each VM */
	private Map<Integer, Integer> currentAllocationCounts;
	private Map<Integer, VirtualMachineState> vmStatesList;
	
	/** Constants and vars */
	private final int tUnder = 50, tUpper = 150;
	
	//Helping vars
	private Deque<Integer> underLoadedNodes;
	private Deque<Integer> overLoadedNodes;
	//pseudo-random number generator
	Random rand;
	
	
	/** 
	 * Constructor
	 * 
	 * @param dcb The {@link DatacenterController} using the load balancer.
	 */
	public ThresholdVmLoadBalancer(DatacenterController dcb){
		dcb.addCloudSimEventListener(this);
		this.vmStatesList = dcb.getVmStatesList();
		rand = new Random();
		initializeUnderLoaded();
		overLoadedNodes = new ArrayDeque<Integer>(vmStatesList.size());;
		this.currentAllocationCounts = Collections.synchronizedMap(new HashMap<Integer, Integer>());
	
	}
	
	private void initializeUnderLoaded(){
		underLoadedNodes = new ArrayDeque<Integer>(vmStatesList.size());
		for (int e : vmStatesList.keySet()){
			underLoadedNodes.push(e);
		}
	}

	/**
	 * @return VM id of the suitable VM from the vmStatesList in the calling
	 * 			{@link DatacenterController}
	 */
	@Override
	public int getNextAvailableVm(){
		int vmId = -1;
		
		if (vmStatesList.size() > 0){
			//random number range 0 - vmStatesList.size()
			int rn = rand.nextInt(vmStatesList.size());
			
			Integer currCount = currentAllocationCounts.remove(rn); //get allocation counts for that node
			if (currCount == null){
				currCount = 1;
			}
			
			if (!(currCount > tUpper)){ //if(not overloaded) then rn
				vmId = rn;
			}else if(!underLoadedNodes.isEmpty()){ //else if(medium loaded node is present)
				vmId = findUnderLoadedNode();
			}else{
				vmId = rn;
				overLoadedNodes.addLast(vmId);
			}
			
			if(vmId == rn){
				if(currCount > 1) currCount++;
				currentAllocationCounts.put(vmId, currCount);
			}else{
				currentAllocationCounts.put(vmId, currCount);
			}
		}
		allocatedVm(vmId);
		return vmId;
	}

	private int findUnderLoadedNode() {
		//find under loaded node and return ID
		//If all available VMs are not allocated, allocated the new ones
		int vmId = underLoadedNodes.pop();
		Integer currCount = currentAllocationCounts.remove(vmId);
		if (currCount == null){
			currCount = 1;
		} else {
			currCount++;
		}
		if(currCount < tUnder){
			underLoadedNodes.addLast(vmId);
		}
		currentAllocationCounts.put(vmId, currCount);
		return vmId;
	}

	public void cloudSimEventFired(CloudSimEvent e) {
		if (e.getId() == CloudSimEvents.EVENT_CLOUDLET_ALLOCATED_TO_VM){
			int vmId = (Integer) e.getParameter(Constants.PARAM_VM_ID);
			Integer currCount = currentAllocationCounts.remove(vmId);
			if (currCount == null){
				currCount = 1;
			} else {
				currCount++;
			}
			currentAllocationCounts.put(vmId, currCount);
			
		} else if (e.getId() == CloudSimEvents.EVENT_VM_FINISHED_CLOUDLET){
			int vmId = (Integer) e.getParameter(Constants.PARAM_VM_ID);
			Integer currCount = currentAllocationCounts.remove(vmId);
			if (currCount != null){
				currCount--;
				currentAllocationCounts.put(vmId, currCount);
				if(currCount < tUnder){
					underLoadedNodes.addLast(vmId);//check VM for under and over load
				}
			}
			if (currCount == null){
				underLoadedNodes.push(vmId);
			}
		}
	}
}
