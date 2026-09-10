package network.arca.sdk

import kotlinx.serialization.Serializable
import network.arca.sdk.models.SimFill
import network.arca.sdk.models.OrderSide

@Serializable
public data class OrderLifecycleFill(
    public val id: String,
    public val orderId: String,
    public val realmId: String,
    public val objectId: String,
    public val operationId: String,
    public val leg: String,
    public val accountId: String,
    public val market: String,
    public val side: String,
    public val size: String,
    public val price: String,
    public val fee: String,
    public val platformFee: String? = null,
    public val builderFee: String? = null,
    public val realizedPnl: String? = null,
    public val createdAt: String? = null,
) {
    internal fun fill(): SimFill = SimFill(id=SimFillId(id),orderId=SimOrderId(orderId),accountId=SimAccountId(accountId),realmId=RealmId(realmId),
        market=market,side=if(side=="buy") OrderSide.BUY else OrderSide.SELL,size=size,price=price,fee=fee,
        platformFee=platformFee,builderFee=builderFee,realizedPnl=realizedPnl,createdAt=createdAt,isLiquidation=false)
}
