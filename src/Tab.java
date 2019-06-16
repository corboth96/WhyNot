import java.util.ArrayList;
import java.util.List;

/**
 * Tab class for making the TabQ instances
 */
public class Tab {
    List<String> input = new ArrayList<>();
    List<String> compatibles = new ArrayList<>();
    List<String> output = new ArrayList<>();
    int level;
    Tab child;
    String name;

    public Tab(List<String> input, List<String> compatibles, int level,Tab child,String name) {
        this.input = input;
        this.compatibles = compatibles;
        this.level = level;
        this.child = child;
        this.name = name;
    }

    public Tab(List<String> compatibles, int level,Tab child,String name) {
        this.compatibles = compatibles;
        this.level = level;
        this.child = child;
        this.name = name;
    }

    public Tab(int level,Tab child,String name) {
        this.level = level;
        this.child = child;
        this.name = name;
    }

    public Tab(int level, String name) {
        this.level = level;
        this.name = name;
    }

}
