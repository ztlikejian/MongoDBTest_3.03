package com.likejian.mongodb;

import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Date;
import java.util.Enumeration;

/**
 * 模拟MongoDB 的ObjectId生成
 * @author likejian
 * 2014年8月5日15:15:50
 */
public class ObjectId implements Comparable<ObjectId> , java.io.Serializable {
 
    static final boolean D = false;
 
    /** Gets a new object id.
     * @return the new id
     */
    public static ObjectId get(){
        return new ObjectId();
    }
    
    public static String getObjectId(){
        return new ObjectId().toString();
    }
 
    /** Checks if a string could be an <code>ObjectId</code>.
     * @return whether the string could be an object id
     */
    public static boolean isValid( String s ){
        if ( s == null )
            return false;
 
        if ( s.length() < 18 )
            return false;
 
        for ( int i=0; i<s.length(); i++ ){
            char c = s.charAt( i );
            if ( c >= '0' && c <= '9' )
                continue;
            if ( c >= 'a' && c <= 'f' )
                continue;
            if ( c >= 'A' && c <= 'F' )
                continue;
 
            return false;
        }        
 
        return true;
    }
 
    /** Turn an object into an <code>ObjectId</code>, if possible.
     * Strings will be converted into <code>ObjectId</code>s, if possible, and <code>ObjectId</code>s will
     * be cast and returned.  Passing in <code>null</code> returns <code>null</code>.
     * @param o the object to convert 
     * @return an <code>ObjectId</code> if it can be massaged, null otherwise 
     */
    public static ObjectId massageToObjectId( Object o ){
        if ( o == null )
            return null;
 
        if ( o instanceof ObjectId )
            return (ObjectId)o;
 
        if ( o instanceof String ){
            String s = o.toString();
            if ( isValid( s ) )
                return new ObjectId( s );
        }
 
        return null;
    }
 
    public ObjectId( Date time ){
        _time = _flip( (int)(time.getTime() / 1000) );
        _machine = _genmachine;
        synchronized ( _incLock ){
            _inc = _nextInc++;
        }     
        _new = false;
    }
 
    public ObjectId( Date time , int inc ){
        this( time , _genmachine , inc );
    }
 
    public ObjectId( Date time , int machine , int inc ){
        _time = _flip( (int)(time.getTime() / 1000) );
        _machine = machine;
        _inc = inc;
        _new = false;
    }
 
 
    /** Creates a new instance from a string.
     * @param s the string to convert
     * @throws IllegalArgumentException if the string is not a valid id
     */
    public ObjectId( String s ){
        this( s , false );
    }
 
    public ObjectId( String s , boolean babble ){
 
        if ( ! isValid( s ) )
            throw new IllegalArgumentException( "invalid ObjectId [" + s + "]" );
 
        if ( babble )
            s = babbleToMongod( s );
 
        byte b[] = new byte[12];
        for ( int i=0; i<b.length; i++ ){
            b[b.length-(i+1)] = (byte)Integer.parseInt( s.substring( i*2 , i*2 + 2) , 16 );
        }
        ByteBuffer bb = ByteBuffer.wrap( b );
 
        _inc = bb.getInt(); 
        _machine = bb.getInt();
        _time = bb.getInt();
 
        _new = false;
    }
 
    public ObjectId( byte[] b ){
        if ( b.length != 12 )
            throw new IllegalArgumentException( "need 12 bytes" );
        reverse( b );
        ByteBuffer bb = ByteBuffer.wrap( b );
 
        _inc = bb.getInt();            
        _machine = bb.getInt();
        _time = bb.getInt();
    }
 
 
    public ObjectId( int time , int machine , int inc ){
        _time = time;
        _machine = machine;
        _inc = inc;
 
        _new = false;
    }
 
    /** Create a new object id.
     */
    public ObjectId(){
        _time = _gentime;
        _machine = _genmachine;
 
        synchronized ( _incLock ){
            _inc = _nextInc++;
        }
 
        _new = true;
    }
 
    public int hashCode(){
        return _inc;
    }
 
    public boolean equals( Object o ){
 
        if ( this == o )
            return true;
 
        ObjectId other = massageToObjectId( o );
        if ( other == null )
            return false;
 
        return 
            _time == other._time && 
            _machine == other._machine && 
            _inc == other._inc;
    }
 
    public String toStringBabble(){
        return babbleToMongod( toStringMongod() );
    }
 
    public String toStringMongod(){
        byte b[] = toByteArray();
 
        StringBuilder buf = new StringBuilder(24);
 
        for ( int i=0; i<b.length; i++ ){
            int x = b[i] & 0xFF;
            String s = Integer.toHexString( x );
            if ( s.length() == 1 )
                buf.append( "0" );
            buf.append( s );
        }
 
        return buf.toString();
    }
 
    public byte[] toByteArray(){
        byte b[] = new byte[12];
        ByteBuffer bb = ByteBuffer.wrap( b );
        bb.putInt( _inc );
        bb.putInt( _machine );
        bb.putInt( _time );
        reverse( b );
        return b;
    }
 
    static void reverse( byte[] b ){
        for ( int i=0; i<b.length/2; i++ ){
            byte t = b[i];
            b[i] = b[ b.length-(i+1) ];
            b[b.length-(i+1)] = t;
        }
    }
 
    static String _pos( String s , int p ){
        return s.substring( p * 2 , ( p * 2 ) + 2 );
    }
 
    public static String babbleToMongod( String b ){
        if ( ! isValid( b ) )
            throw new IllegalArgumentException( "invalid object id: " + b );
 
        StringBuilder buf = new StringBuilder( 24 );
        for ( int i=7; i>=0; i-- )
            buf.append( _pos( b , i ) );
        for ( int i=11; i>=8; i-- )
            buf.append( _pos( b , i ) );
 
        return buf.toString();
    }
 
    public String toString(){
        return toStringMongod();
    }
 
    public int compareTo( ObjectId id ){
        if ( id == null )
            return -1;
 
        long xx = id.getTime() - getTime();
        if ( xx > 0 )
            return -1;
        else if ( xx < 0 )
            return 1;
 
        int x = id._machine - _machine;
        if ( x != 0 )
            return -x;
 
        x = id._inc - _inc;
        if ( x != 0 )
            return -x;
 
        return 0;
    }
 
    public int getMachine(){
        return _machine;
    }
 
    public long getTime(){
        long z = _flip( _time );
        return z * 1000;
    }
 
    public int getInc(){
        return _inc;
    }
 
    public int _time(){
        return _time;
    }
    public int _machine(){
        return _machine;
    }
    public int _inc(){
        return _inc;
    }
 
    public boolean isNew(){
        return _new;
    }
 
    public void notNew(){
        _new = false;
    }
 
    final int _time;
    final int _machine;
    final int _inc;
 
    boolean _new;
 
    static int _flip( int x ){
        if ( true ){
            byte b[] = new byte[4];
            ByteBuffer bb = ByteBuffer.wrap( b );
            bb.order( ByteOrder.LITTLE_ENDIAN );
            bb.putInt( x );
            bb.flip();
            bb.order( ByteOrder.BIG_ENDIAN );
            return bb.getInt();
        }
        int z = 0;
        z |= ( x & 0xFF ) << 24;
        z |= ( x & 0xFF00 ) << 8;
        z |= ( x & 0xFF00000 ) >> 8;
        z |= ( x & 0xFF000000 ) >> 24;
        return z;
    }
 
    private static int _nextInc = (new java.util.Random()).nextInt();
    private static final String _incLock = new String( "ObjectId._incLock" );
 
    private static int _gentime = _flip( (int)(System.currentTimeMillis()/1000) );
 
    static final Thread _timeFixer;
    private static final int _genmachine;
    static {
 
        try {
 
            final int machinePiece;
            {
                StringBuilder sb = new StringBuilder();
                Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
                while ( e.hasMoreElements() ){
                    NetworkInterface ni = e.nextElement();
                    sb.append( ni.toString() );
                }
                machinePiece = sb.toString().hashCode() << 16;
                if ( D ) System.out.println( "machine piece post: " + Integer.toHexString( machinePiece ) );
            }
 
            final int processPiece = java.lang.management.ManagementFactory.getRuntimeMXBean().getName().hashCode() & 0xFFFF;
            if ( D ) System.out.println( "process piece: " + Integer.toHexString( processPiece ) );
 
            _genmachine = machinePiece | processPiece;
            if ( D ) System.out.println( "machine : " + Integer.toHexString( _genmachine ) );
        }
        catch ( java.io.IOException ioe ){
            throw new RuntimeException( ioe );
        }
 
        _timeFixer = new Thread("ObjectId-TimeFixer"){
                public void run(){
                    while ( true ){
                        try {
                            Thread.sleep( 499 );
                        }
                        catch ( Exception e ){}
                        _gentime = _flip( (int)(System.currentTimeMillis()/1000) );
                    }
                }
            };
        _timeFixer.setDaemon( true );
        _timeFixer.start();
    }
 
}