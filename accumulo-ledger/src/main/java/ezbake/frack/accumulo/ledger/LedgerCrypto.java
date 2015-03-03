/*   Copyright (C) 2013-2015 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.frack.accumulo.ledger;

import ezbake.common.openshift.OpenShiftUtil;
import ezbake.common.properties.EzProperties;
import ezbake.crypto.PKeyCryptoException;
import ezbake.crypto.RSAKeyCrypto;
import ezbake.thrift.ThriftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.BadPaddingException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.IllegalBlockSizeException;

import org.apache.thrift.TException;
import org.bouncycastle.util.Strings;
import ezbake.frack.core.thrift.SecureMessage;

/**
 * Class used by Ledger to encrypt and decrpyt messages.
 */
public class LedgerCrypto { 

	private RSAKeyCrypto crypto;
	protected boolean isProduction = false;

	private static final String ALGO = "AES";
	private static final String PRIV_KEY_FILEPATH = "/ssl/application.priv";
	private static final String PRODUCTION_MODE = "accumulo.ledger.security.production";

	private static final Logger log = LoggerFactory.getLogger(LedgerCrypto.class);

	public LedgerCrypto(Properties props) {
		// If in an OpenShift container, always assume in production
		this.isProduction = OpenShiftUtil.inOpenShiftContainer() || new EzProperties(props, true).getBoolean(PRODUCTION_MODE, true);

		if (this.isProduction) {
			this.initializeEncryption();	
		} else {
			log.warn("AccumuloLedger is running in local mode. Encryption and security checking is off");
		}
	}

	private void initializeEncryption() { 
		try {
			// Try to get the private key if it exists
			InputStream is = this.getClass().getResourceAsStream(LedgerCrypto.PRIV_KEY_FILEPATH);

			if (is == null) {
				log.warn("Did not find private key: " + LedgerCrypto.PRIV_KEY_FILEPATH);
			} 

			byte[] bytes = ByteStreams.toByteArray(is);
			String key = Strings.fromByteArray(bytes);

			if (bytes.length > 0) {
				crypto = new RSAKeyCrypto(key, true);
				return;
			} else {
				log.warn("No private key file found, messages will not be decrypted");
			}
		} catch (Exception e) {
			log.error("Could not initialize encryption.", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * This method encrypts a message.
	 *
	 * @param payload the payload of the message
	 * @return byte[] 
	 */
	public byte[] encrypt(byte[] payload) throws IOException {
		byte[] serializedMessage = null;
		byte[] payloadToSend = payload;
		SecureMessage message = new SecureMessage();

		try { 
			if (isProduction) { 
				// Generate a new symmetric key and encrypt the message
				Cipher cipher = Cipher.getInstance(ALGO);
				KeyGenerator gen = KeyGenerator.getInstance(ALGO);
				gen.init(256);
				SecretKey key = gen.generateKey();
				cipher.init(Cipher.ENCRYPT_MODE, key);
				payloadToSend = cipher.doFinal(payload);

				// Encrypt the key and embed it into the message
				message.setKey(this.crypto.encrypt(key.getEncoded()));
			}

			message.setContent(payloadToSend);
			serializedMessage = ThriftUtils.serialize(message);
		} catch (TException e) {
			throw new IOException("Could not serialize message", e);
		} catch (Exception e) {
			log.error("Could not encrypt message.", e);
			throw new RuntimeException(e);
		}

		return serializedMessage;
	}

	/**
	 * This method decrypts a message.
	 *
	 * @param payload the payload of the message
	 * @return byte[]
	 */
	protected byte[] decrypt(byte[] payload) throws IOException {
		byte[] content = null;
		SecureMessage message;

		try { 
			message = ThriftUtils.deserialize(SecureMessage.class, payload);
			if (message.isSetKey()) {
				if (this.crypto != null && this.crypto.hasPrivate()) {
						// Decrypt the symmetric key
						byte[] symmetricKey = this.crypto.decrypt(message.getKey());

						// Use the symmetric key to decrypt the message
						SecretKey key = new SecretKeySpec(symmetricKey, ALGO);
						Cipher cipher = Cipher.getInstance(ALGO);
						cipher.init(Cipher.DECRYPT_MODE, key);
						content = cipher.doFinal(message.getContent());
				} else {
					String error = "No private key found for this app. Cannot decrypt messages. Please re-initialize with a private key to receive messages.";
					log.error(error);
					throw new RuntimeException(error);
				}
			} else {
				log.debug("Message was not encrypted, or no key was set");
			}
		} catch (IllegalBlockSizeException | InvalidKeyException | BadPaddingException e) {
			log.error("Encryption not set up properly, this error is fatal.", e);
			throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException e) {
			log.error("Incorrect algorithm used to initialize crypto", e);
			throw new RuntimeException(e);
		} catch (NoSuchPaddingException e) {
			log.error("Incorrect padding used to initialize crypto", e);
			throw new RuntimeException(e);
		} catch (PKeyCryptoException e) {
			log.error("Invalid crypto object", e);
			throw new RuntimeException(e);
		} catch (TException e) {
			throw new IOException("Could not serialize message", e);
		}
		return content;
	}
}
