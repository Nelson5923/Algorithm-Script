package PageRank;
import java.util.*;

public class PRNodeWritable {

    public int nodeID;
    public double PageRank;
    public ArrayList<Integer> AdjacentList;
    
    public PRNodeWritable() {
        this.nodeID = -1;
        this.PageRank = 0;
        this.AdjacentList = new ArrayList<Integer>();
    }
    
    public void setNodeID(int value){
        nodeID = value;
    }
    
    public void addList(int value) {
        AdjacentList.add(value);
    }

    public void setPageRank(double value) {
        PageRank = value;
    }
 
    public int getNodeID(){
        return nodeID;
    }
    
    public double getPageRank() {
        return PageRank;
    }
    
    public String getList(){
        
        StringBuffer sb = new StringBuffer();
        for(Integer d : AdjacentList)
            sb.append(d.toString()).append(" ");
        return sb.toString().trim();
        
    }
    
    public Integer getListLength(){
        return AdjacentList.size();
    }
    
    public void clearList() {
        AdjacentList.clear();
    }    

}
