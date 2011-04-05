package org.gusdb.workflow;

// to make digester easy.  probably a better way, involving tinkering w/
// digester
public class Name implements Comparable<Name> {
    String name;
    public void setName(String n) { name = n; }
    public String getName() { return name; }
    public int compareTo(Name arg0) {
        return name.compareTo(arg0.name);
    }
    public String toString() { return name;}
}

