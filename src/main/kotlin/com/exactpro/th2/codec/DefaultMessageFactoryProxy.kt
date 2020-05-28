package com.exactpro.th2.codec

import com.exactpro.sf.common.impl.messages.DefaultMessageFactory.getFactory
import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.configuration.suri.SailfishURI
import com.exactpro.sf.externalapi.IMessageFactoryProxy

class DefaultMessageFactoryProxy : IMessageFactoryProxy {
    override fun createMessage(dictionary: SailfishURI?, name: String?): IMessage {
        return getFactory().createMessage(name, dictionary?.resourceName)
    }

    override fun createStrictMessage(dictionary: SailfishURI?, name: String?): IMessage {
        return getFactory().createMessage(name, dictionary?.resourceName)
    }
}