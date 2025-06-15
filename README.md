# Studio_Ghibli_ETL
My third ETL project. 

__Base URL__ - https://ghibliapi.vercel.app/

## **Flow**
1. Extracting data from **Base URL** using *requests(python)*
2. Transforming using *pandas(python)*
3. Loading it into _mongodb_ using _pymongo(python)_


## Must configure mongodb to accept connections from all IPs
Here's how to configure MongoDB to accept external connections (Step 1):

### **Step 1: Configure MongoDB to Accept External Connections**

1. **Locate MongoDB Configuration File**  
   The configuration file is typically found at:
   ```
   /etc/mongod.conf
   ```
   (Use `sudo` to edit it if needed)

2. **Edit the Configuration File**  
   Open it with a text editor like `nano`:
   ```bash
   sudo nano /etc/mongod.conf
   ```

3. **Modify the `bindIp` Setting**  
   Look for the `net` section and change:
   ```yaml
   net:
     port: 27017
     bindIp: 127.0.0.1  # Default (only allows local connections)
   ```
   To:
   ```yaml
   net:
     port: 27017
     bindIp: 0.0.0.0    # Allow connections from any IP
   ```

4. **Save and Exit**  
   - In `nano`: Press `CTRL+X` → `Y` → `Enter`.

5. **Restart MongoDB Service**  
   ```bash
   sudo systemctl restart mongod
   ```

6. **Verify MongoDB is Running**  
   ```bash
   sudo systemctl status mongod
   ```
   You should see `active (running)` in the output.

---

### **Important Security Note**  
Binding to `0.0.0.0` opens MongoDB to the network. To secure it:
1. Enable authentication in `mongod.conf`:
   ```yaml
   security:
     authorization: enabled
   ```
2. Create a MongoDB user with access controls.
3. Use a firewall (e.g., `ufw`) to restrict access to trusted IPs:
   ```bash
   sudo ufw allow from 192.168.1.0/24 to any port 27017  # Example for local network
   ```