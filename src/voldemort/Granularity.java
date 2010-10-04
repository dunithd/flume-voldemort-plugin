package voldemort;

/**
 * Enumerates the key space granularity levels
 * DAY - Key will be in format yyyyMMdd
 * HOUR - Key will be in format yyyyMMddHH
 * MINUTE - Key will be in format yyyyMMddHHmm
 */
public enum Granularity {
    DAY("day"),
    HOUR("hour"),
    MINUTE("minute");

    private final String text;

    private Granularity(String text) {
        this.text = text;
    }

    public static Granularity fromDisplay(String type) {
        for(Granularity g : Granularity.values())
            if(g.toString().equals(type))
                return g;
        throw new IllegalArgumentException("No Granularity type " + type + " exists.");
    }

    public String toString() {
        return this.text;
    }
}
